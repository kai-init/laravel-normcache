<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Planning\CascadeDependencyResolver;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use NormCache\Values\TableIdentity;

final class CascadeNote extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $guarded = [];
}

final class PrefixedCascadeParent extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $connection = 'cascade-prefix';

    protected $table = 'cascade_parents';

    protected $guarded = [];
}

final class PrefixedCascadeChild extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $connection = 'cascade-prefix';

    protected $table = 'cascade_children';

    protected $guarded = [];
}

final class CascadeInvalidationTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        if (DB::connection()->getDriverName() === 'sqlite') {
            DB::statement('PRAGMA foreign_keys = ON');
        }
    }

    public function test_parent_delete_generation_invalidates_cascaded_child_rows(): void
    {
        [$author, $post] = $this->authorAndPost();
        $this->warmCascadeGraph();
        $identity = $this->identity('posts');
        $generation = $this->generation($identity);

        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);
        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);

        $author->delete();

        $this->assertSame($generation + 1, $this->generation($identity));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertNull(Post::query()->find($post->getKey()));
        DB::disableQueryLog();

        $this->assertCount(
            1,
            DB::getQueryLog(),
            json_encode(array_column(DB::getQueryLog(), 'query')) ?: 'Could not encode query log.',
        );
    }

    public function test_set_null_delete_action_generation_invalidates_changed_children(): void
    {
        $country = Country::query()->create(['name' => 'Country']);
        $author = Author::query()->create([
            'name' => 'Author',
            'country_id' => $country->getKey(),
        ]);
        $this->warmCascadeGraph();
        $identity = $this->identity('authors');
        $generation = $this->generation($identity);

        $this->assertSame(
            $country->getKey(),
            Author::query()->findOrFail($author->getKey())->country_id,
        );

        $country->delete();

        $this->assertSame($generation + 1, $this->generation($identity));
        $this->assertNull(Author::query()->findOrFail($author->getKey())->country_id);
    }

    public function test_cascade_delete_recurses_through_multiple_foreign_keys(): void
    {
        Schema::create('cascade_notes', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('post_id')->constrained('posts')->cascadeOnDelete();
            $table->string('body');
        });

        [$author, $post] = $this->authorAndPost();
        $note = CascadeNote::query()->create([
            'post_id' => $post->getKey(),
            'body' => 'Cached grandchild',
        ]);
        $this->warmCascadeGraph();
        $identity = $this->identity('cascade_notes');
        $generation = $this->generation($identity);

        $this->assertSame(
            'Cached grandchild',
            CascadeNote::query()->findOrFail($note->getKey())->body,
        );

        $author->delete();

        $this->assertSame($generation + 1, $this->generation($identity));
        $this->assertNull(CascadeNote::query()->find($note->getKey()));
    }

    public function test_cascade_invalidations_wait_for_commit(): void
    {
        [$author, $post] = $this->authorAndPost();
        $this->warmCascadeGraph();
        $identity = $this->identity('posts');
        $generation = $this->generation($identity);

        Post::query()->findOrFail($post->getKey());

        DB::beginTransaction();
        $author->delete();
        $this->assertSame($generation, $this->generation($identity));
        DB::commit();

        $this->assertSame($generation + 1, $this->generation($identity));
        $this->assertNull(Post::query()->find($post->getKey()));
    }

    public function test_rolled_back_cascade_discards_child_invalidation(): void
    {
        [$author, $post] = $this->authorAndPost();
        $this->warmCascadeGraph();
        $identity = $this->identity('posts');
        $generation = $this->generation($identity);

        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);

        DB::beginTransaction();
        $author->delete();
        DB::rollBack();

        $this->assertSame($generation, $this->generation($identity));
        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);
    }

    public function test_persisted_graph_avoids_schema_inspection_in_a_new_scope(): void
    {
        [$firstAuthor] = $this->authorAndPost();
        [$secondAuthor] = $this->authorAndPost();
        [$thirdAuthor, $thirdPost] = $this->authorAndPost();

        $firstAuthor->delete();
        $secondAuthor->delete();
        $this->app->forgetScopedInstances();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $thirdAuthor->delete();
        DB::disableQueryLog();

        $this->assertCount(
            1,
            DB::getQueryLog(),
            json_encode(array_column(DB::getQueryLog(), 'query')) ?: 'Could not encode query log.',
        );
        $this->assertNull(Post::query()->find($thirdPost->getKey()));
    }

    public function test_schema_refresh_rebuilds_the_cascade_graph(): void
    {
        [$firstAuthor] = $this->authorAndPost();
        $firstAuthor->delete();

        Schema::create('cascade_notes', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('post_id')->constrained('posts')->cascadeOnDelete();
            $table->string('body');
        });

        $this->assertTrue($this->cacheManager()->refreshSchema('testing'));

        [$author, $post] = $this->authorAndPost();
        $note = CascadeNote::query()->create([
            'post_id' => $post->getKey(),
            'body' => 'Added after discovery',
        ]);
        CascadeNote::query()->findOrFail($note->getKey());

        $author->delete();

        $this->assertNull(CascadeNote::query()->find($note->getKey()));
    }

    public function test_truncate_globally_invalidates_other_table_caches(): void
    {
        [, $post] = $this->authorAndPost();
        $this->assertNotNull(Post::query()->find($post->getKey()));
        $this->assertNotNull(Post::query()->find($post->getKey()));
        $epoch = $this->epoch();

        DB::table('uuid_items')->truncate();

        $this->assertSame($epoch + 1, $this->epoch());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertNotNull(Post::query()->find($post->getKey()));
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_transactional_truncate_invalidates_immediately_and_again_on_commit(): void
    {
        if (!in_array(DB::connection()->getDriverName(), ['pgsql', 'sqlite'], true)) {
            $this->markTestSkipped('This database does not keep TRUNCATE inside the transaction.');
        }

        $epoch = $this->epoch();

        DB::beginTransaction();
        DB::table('uuid_items')->truncate();
        $this->assertSame($epoch + 1, $this->epoch());
        DB::commit();

        $this->assertSame($epoch + 2, $this->epoch());
    }

    public function test_rolled_back_transactional_truncate_discards_the_second_invalidation(): void
    {
        if (!in_array(DB::connection()->getDriverName(), ['pgsql', 'sqlite'], true)) {
            $this->markTestSkipped('This database does not keep TRUNCATE inside the transaction.');
        }

        $epoch = $this->epoch();

        DB::beginTransaction();
        DB::table('uuid_items')->truncate();
        $this->assertSame($epoch + 1, $this->epoch());
        DB::rollBack();

        DB::beginTransaction();
        DB::commit();

        $this->assertSame($epoch + 1, $this->epoch());
    }

    public function test_prefixed_connection_discovers_and_invalidates_cascaded_children(): void
    {
        $name = 'cascade-prefix';
        $config = (array) config('database.connections.testing');
        $config['prefix'] = 'app_';
        config()->set("database.connections.{$name}", $config);
        DB::purge($name);
        $connection = DB::connection($name);

        if ($connection->getDriverName() === 'sqlite') {
            $connection->statement('PRAGMA foreign_keys = ON');
        }

        $schema = $connection->getSchemaBuilder();

        try {
            $schema->create('cascade_parents', function (Blueprint $table): void {
                $table->id();
                $table->string('name');
            });
            $schema->create('cascade_children', function (Blueprint $table): void {
                $table->id();
                $table->foreignId('parent_id')
                    ->constrained('cascade_parents')
                    ->cascadeOnDelete();
            });

            $parent = PrefixedCascadeParent::query()->create(['name' => 'Parent']);
            $child = PrefixedCascadeChild::query()->create([
                'parent_id' => $parent->getKey(),
            ]);
            $identity = $this->app
                ->make(TableIdentityResolver::class)
                ->resolve($connection, 'cascade_children')
                ?? throw new \RuntimeException('Could not resolve the prefixed child table.');
            $this->warmCascadeGraph($connection);
            $generation = $this->generation($identity);

            $this->assertNotNull(PrefixedCascadeChild::query()->find($child->getKey()));
            $parent->delete();

            $this->assertSame($generation + 1, $this->generation($identity));
            $this->assertNull(PrefixedCascadeChild::query()->find($child->getKey()));
        } finally {
            $schema->dropIfExists('cascade_children');
            $schema->dropIfExists('cascade_parents');
            DB::purge($name);
        }
    }

    public function test_cold_delete_globally_invalidates_then_warms_the_graph(): void
    {
        [$author, $post] = $this->authorAndPost();
        Post::query()->findOrFail($post->getKey());
        $epoch = $this->epoch();
        $author->delete();

        $this->assertSame($epoch + 1, $this->epoch());
        $this->assertNull(Post::query()->find($post->getKey()));
        $graph = $this->app->make(SchemaRepository::class)->deleteActions(DB::connection());

        $this->assertNotNull($graph);
        $this->assertArrayHasKey(
            $this->identity('authors')->hash,
            $graph,
            json_encode($graph) ?: 'Could not encode the cascade graph.',
        );

        [$nextAuthor, $nextPost] = $this->authorAndPost();
        $identity = $this->identity('posts');
        $generation = $this->generation($identity);
        Post::query()->findOrFail($nextPost->getKey());

        $nextAuthor->delete();

        $this->assertSame($generation + 1, $this->generation($identity));
        $this->assertNull(Post::query()->find($nextPost->getKey()));
    }

    public function test_cold_graph_build_contention_uses_global_invalidation_without_scanning(): void
    {
        [$author, $post] = $this->authorAndPost();
        Post::query()->findOrFail($post->getKey());
        $schema = $this->app->make(SchemaRepository::class);
        $buildingKey = $schema->deleteActionsBuildKey(DB::connection());
        $this->assertTrue($this->cacheStore()->setNxEx($buildingKey, str_repeat('a', 32), 30));
        $epoch = $this->epoch();

        try {
            DB::flushQueryLog();
            DB::enableQueryLog();
            $author->delete();
            DB::disableQueryLog();

            $this->assertCount(1, DB::getQueryLog());
            $this->assertSame($epoch + 1, $this->epoch());
            $this->assertNull(Post::query()->find($post->getKey()));
        } finally {
            $this->cacheStore()->delete($buildingKey);
        }
    }

    /** @return array{Author, Post} */
    private function authorAndPost(): array
    {
        $author = Author::query()->create(['name' => 'Parent']);
        $post = Post::query()->create([
            'title' => 'Cached child',
            'author_id' => $author->getKey(),
        ]);

        return [$author, $post];
    }

    private function identity(string $table): TableIdentity
    {
        return $this->app
            ->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $table)
            ?? throw new \RuntimeException("Could not resolve [{$table}].");
    }

    private function generation(TableIdentity $table): int
    {
        return (int) ($this->cacheStore()->getRaw(
            $this->cacheKeys()->generation($table),
        ) ?? '0');
    }

    private function epoch(): int
    {
        return (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');
    }

    private function warmCascadeGraph(?Connection $connection = null): void
    {
        $this->app
            ->make(CascadeDependencyResolver::class)
            ->warm($connection ?? DB::connection());
    }
}
