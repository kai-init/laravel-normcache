<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Database\DatabaseTransactionsManager;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\DB;
use NormCache\Database\QueryBuilder;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use NormCache\Values\PrimaryKeyMetadata;

final class ForeignTransactionsManager extends DatabaseTransactionsManager {}

final class PseudoSoftDeletePost extends Model
{
    use Cacheable;

    protected $table = 'posts';

    public function getDeletedAtColumn(): string
    {
        return 'archived_at';
    }
}

final class ConnectionWiringTest extends TestCase
{
    public function test_db_table_and_db_query_use_laravel_query_builders(): void
    {
        $this->assertSame(Builder::class, DB::table('authors')::class);
        $this->assertSame(Builder::class, DB::query()::class);
    }

    public function test_connection_remains_laravels_configured_connection(): void
    {
        $this->assertSame(SQLiteConnection::class, DB::connection()::class);
    }

    public function test_laravels_transactions_manager_is_left_in_place(): void
    {
        $this->assertSame(
            DatabaseTransactionsManager::class,
            $this->app->make('db.transactions')::class,
        );
    }

    public function test_invalidation_still_publishes_under_a_replacement_transactions_manager(): void
    {
        $this->app->instance('db.transactions', new ForeignTransactionsManager);
        DB::connection()->setTransactionManager($this->app->make('db.transactions'));

        $post = Post::query()->create([
            'title' => 'Before',
            'author_id' => Author::query()->create(['name' => 'Author'])->getKey(),
        ]);
        $read = fn(): ?string => Post::query()->toBase()->where('id', $post->getKey())->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());
        $observed = null;

        DB::transaction(function () use ($post, $read, &$observed): void {
            Post::query()->toBase()->where('id', $post->getKey())->update(['title' => 'Committed']);

            DB::afterCommit(function () use ($read, &$observed): void {
                $observed = $read();
            });
        });

        $this->assertSame('Committed', $observed);
        $this->assertSame('Committed', $read());
    }

    public function test_only_cacheable_models_receive_normcache_query_builders(): void
    {
        $this->assertInstanceOf(QueryBuilder::class, Post::query()->toBase());
        $this->assertSame(Builder::class, UncachedPost::query()->toBase()::class);
    }

    public function test_cacheable_model_metadata_is_the_authoritative_primary_key_source(): void
    {
        $integer = Post::query()->toBase();
        $string = UuidItem::query()->toBase();

        $this->assertSame(Post::class, $integer->modelClass());
        $this->assertSame('id', $integer->primaryKey()?->column);
        $this->assertSame(PrimaryKeyMetadata::INTEGER, $integer->primaryKey()?->family);
        $this->assertSame(PrimaryKeyMetadata::STRING, $string->primaryKey()?->family);
        $this->assertSame('deleted_at', $integer->deletedAtColumn());
    }

    public function test_only_a_soft_deleting_model_reports_a_deleted_at_column(): void
    {
        $this->assertNull(Author::query()->toBase()->deletedAtColumn());
        $this->assertNull(
            (new PseudoSoftDeletePost)->newQuery()->toBase()->deletedAtColumn(),
            'defining getDeletedAtColumn() without the SoftDeletes trait does not soft delete',
        );
    }

    public function test_laravel_created_pivot_builder_does_not_reuse_root_model_metadata(): void
    {
        $pivot = Post::query()->toBase()->newQuery()->from('post_tag');

        $this->assertInstanceOf(QueryBuilder::class, $pivot);
        $this->assertNull($pivot->primaryKey());
        $this->assertNull($pivot->modelClass());
    }

    public function test_cacheable_model_works_with_a_user_provided_connection_subclass(): void
    {
        $name = 'user-provided';
        $database = (string) DB::connection()->getDatabaseName();

        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
            'name' => $name,
        ]);
        DB::extend($name, static fn(array $config) => new class(new \PDO('sqlite:' . $database), $database, '', $config) extends SQLiteConnection {});
        DB::purge($name);

        try {
            $connection = DB::connection($name);
            $builder = Post::on($name)->toBase();

            $this->assertNotSame(SQLiteConnection::class, $connection::class);
            $this->assertInstanceOf(QueryBuilder::class, $builder);
            $this->assertSame($connection, $builder->getConnection());
        } finally {
            DB::disconnect($name);
            DB::purge($name);
            DB::forgetExtension($name);
        }
    }
}
