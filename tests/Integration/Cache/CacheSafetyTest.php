<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use NormCache\Cache\BuildLeaseCoordinator;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\CacheStateResolver;
use NormCache\Cache\CanonicalRowRepository;
use NormCache\Events\QueryCacheMiss;
use NormCache\Planning\DeleteDependencyResolver;
use NormCache\Planning\DependencyAnalyzer;
use NormCache\Planning\QueryPlanner;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;

class CacheSafetyModel extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $guarded = [];
}

final class CacheSafetyTest extends TestCase
{
    protected function tearDown(): void
    {
        DB::statement('drop view if exists review_author_view');
        Schema::dropIfExists('review_children');

        parent::tearDown();
    }

    public function test_projection_order_by_position_preserves_database_result(): void
    {
        Author::create(['name' => 'Zulu']);
        Author::create(['name' => 'Alpha']);
        Author::query()->orderByRaw('1')->limit(1)->get();

        $query = fn() => Author::query()->select('name', 'id')->orderByRaw('1')->limit(1);
        $expected = $query()->withoutCache()->get()->toArray();
        $actual = $query()->get()->toArray();

        $this->assertSame($expected, $actual);
    }

    public function test_direct_pk_waiter_consumes_published_row(): void
    {
        $author = Author::create(['name' => 'Alice']);
        $query = Author::query()->whereKey($author->id)->limit(1)->toBase();
        $analysis = app(DependencyAnalyzer::class)->analyze($query->getConnection(), $query);
        $plan = app(QueryPlanner::class)->plan($query, $analysis->root, $analysis->tables);
        $state = app(CacheStateResolver::class)->resolve($plan, 'u', 'unused');
        $lease = app(BuildLeaseCoordinator::class)->claim($plan, $state, 'u', 'unused');
        $row = DB::table('authors')->where('id', $author->id)->first();
        Event::listen(QueryCacheMiss::class, function () use ($plan, $state, $row, $lease): void {
            app(CanonicalRowRepository::class)->publish($plan, $state, [$row], $lease);
        });

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Alice', Author::find($author->id)->name);
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame([], $queries, 'The owner published and woke the waiter before its retry.');
    }

    public function test_external_migration_epoch_refreshes_cascade_graph(): void
    {
        if (DB::connection()->getDriverName() === 'sqlite') {
            DB::statement('PRAGMA foreign_keys = ON');
        }

        $author = Author::create(['name' => 'Alice']);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        app(DeleteDependencyResolver::class)->affectedByDelete(DB::connection(), $table, app(CacheRuntime::class)->epoch());

        Schema::create('review_children', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('author_id')->constrained('authors')->cascadeOnDelete();
        });
        $this->cacheStore()->increment($this->cacheKeys()->epoch());
        $this->app->forgetScopedInstances();
        $model = (new CacheSafetyModel)->setTable('review_children');
        $child = $model->newQuery()->create(['author_id' => $author->id]);
        $this->assertNotNull($model->newQuery()->find($child->id));

        $this->app->forgetScopedInstances();
        $author->delete();

        $this->assertNull($model->newQuery()->withoutCache()->find($child->id));
        $this->assertNull($model->newQuery()->find($child->id));
    }

    public function test_declared_view_dependencies_use_invalidated_full_results(): void
    {
        $author = Author::create(['name' => 'Before']);
        DB::statement('create view review_author_view as select * from authors');
        $model = (new CacheSafetyModel)->setTable('review_author_view');
        $read = fn() => $model->newQuery()->dependsOn([Author::class])->find($author->id);

        $this->assertSame('Before', $read()->name);
        $this->assertWarmCacheHit($read);
        $this->assertSame([], $this->cacheKeysMatching(':r:g'));
        $author->update(['name' => 'After']);
        $this->assertSame('After', $read()->name);
        $this->assertWarmCacheHit($read);
    }
}
