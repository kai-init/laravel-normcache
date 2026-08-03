<?php

namespace NormCache\Tests\Review;

use Illuminate\Database\SQLiteConnection;
use Illuminate\Redis\Connections\Connection as RedisConnection;
use Illuminate\Redis\Events\CommandExecuted;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Database\Connections\BuildsCachingQueries;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheSerializer;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

final class AppliedThenThrowsSQLiteConnection extends SQLiteConnection
{
    use BuildsCachingQueries;

    public bool $throwAfterNextAffectingStatement = false;

    public function affectingStatement($query, $bindings = [])
    {
        $affected = parent::affectingStatement($query, $bindings);

        if ($this->throwAfterNextAffectingStatement) {
            $this->throwAfterNextAffectingStatement = false;

            throw new \RuntimeException('The database applied the write but the response was lost.');
        }

        return $affected;
    }
}

final class MisconfiguredPrimaryKeyPost extends Post
{
    protected $table = 'posts';

    protected $primaryKey = 'missing_id';
}

final class CodeReviewIssueReproductionTest extends TestCase
{
    public function test_table_identity_collides_across_distinct_connection_names(): void
    {
        $shardA = TableIdentity::fromParts(
            driver: 'mysql',
            connection: 'shard-a',
            database: 'app',
            schema: 'app',
            prefix: '',
            table: 'posts',
        );
        $shardB = TableIdentity::fromParts(
            driver: 'mysql',
            connection: 'shard-b',
            database: 'app',
            schema: 'app',
            prefix: '',
            table: 'posts',
        );

        $this->assertNotSame($shardA->connection, $shardB->connection);
        $this->assertSame($shardA->encoded, $shardB->encoded);
        $this->assertSame($shardA->hash, $shardB->hash);
    }

    public function test_opaque_query_builder_write_leaves_a_warm_row_stale(): void
    {
        [, $post] = $this->createPost('Before');
        $read = static fn() => DB::table('posts')->where('id', $post->getKey())->first();

        $this->assertSame('Before', $read()?->title);
        $this->assertSame('Before', $read()?->title);

        DB::table(DB::raw('posts'))
            ->where('id', $post->getKey())
            ->update(['title' => 'After']);

        $cached = $read();
        $live = DB::table('posts')
            ->withoutCache()
            ->where('id', $post->getKey())
            ->first();

        $this->assertSame('Before', $cached?->title);
        $this->assertSame('After', $live?->title);
    }

    public function test_applied_write_that_throws_leaves_a_warm_row_stale(): void
    {
        [, $post] = $this->createPost('Before');
        $name = 'uncertain-write';
        $database = (string) DB::connection()->getDatabaseName();

        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
        ]);
        DB::extend($name, static fn(array $config) => new AppliedThenThrowsSQLiteConnection(
            new \PDO('sqlite:' . $database),
            $database,
            '',
            $config,
        ));
        DB::purge($name);

        try {
            $connection = DB::connection($name);
            $this->assertInstanceOf(AppliedThenThrowsSQLiteConnection::class, $connection);
            $read = static fn() => $connection
                ->table('posts')
                ->where('id', $post->getKey())
                ->first();

            $this->assertSame('Before', $read()?->title);
            $this->assertSame('Before', $read()?->title);

            $connection->throwAfterNextAffectingStatement = true;

            try {
                $connection
                    ->table('posts')
                    ->where('id', $post->getKey())
                    ->update(['title' => 'After']);
                $this->fail('The simulated lost response was not thrown.');
            } catch (\RuntimeException $exception) {
                $this->assertSame(
                    'The database applied the write but the response was lost.',
                    $exception->getMessage(),
                );
            }

            $cached = $read();
            $live = $connection
                ->table('posts')
                ->withoutCache()
                ->where('id', $post->getKey())
                ->first();

            $this->assertSame('Before', $cached?->title);
            $this->assertSame('After', $live?->title);
        } finally {
            DB::disconnect($name);
            DB::purge($name);
            DB::forgetExtension($name);
        }
    }

    public function test_foreign_key_cascade_leaves_the_child_row_cache_stale(): void
    {
        DB::statement('PRAGMA foreign_keys = ON');
        [$author, $post] = $this->createPost('Cascade target');
        $read = static fn() => Post::query()->find($post->getKey());

        $this->assertSame('Cascade target', $read()?->title);
        $this->assertSame('Cascade target', $read()?->title);

        Author::query()->whereKey($author->getKey())->delete();

        $cached = $read();
        $live = Post::withoutCache()->find($post->getKey());

        $this->assertSame('Cascade target', $cached?->title);
        $this->assertNull($live);
    }

    public function test_automatic_serializer_selection_is_not_cross_node_compatible(): void
    {
        if (!extension_loaded('igbinary')) {
            $this->markTestSkipped('The igbinary extension is required to reproduce mixed serializer nodes.');
        }

        $php = new CacheSerializer(false);
        $igbinary = new CacheSerializer(true);
        $value = [
            'id' => 42,
            'title' => 'serializer probe',
            'nested' => ['a', 'b', 'c'],
        ];

        $this->assertNotSame($value, $igbinary->decode($php->encode($value)));
        $this->assertNotSame($value, $php->decode($igbinary->encode($value)));
    }

    public function test_retrying_an_ambiguous_increment_can_apply_it_twice(): void
    {
        $original = $this->app->make('redis');
        $manager = new class
        {
            public int $built = 0;

            public int $value = 0;

            /** @var list<string> */
            public array $purged = [];

            public function connection($name = null): RedisConnection
            {
                $attempt = $this->built++;
                $manager = $this;
                $client = new class($manager, $attempt)
                {
                    public function __construct(
                        private object $manager,
                        private int $attempt,
                    ) {}

                    /** @param list<mixed> $arguments */
                    public function __call(string $method, array $arguments): mixed
                    {
                        if ($method !== 'incr') {
                            return null;
                        }

                        $this->manager->value++;

                        if ($this->attempt === 0) {
                            throw new \RuntimeException('Connection lost after Redis applied INCR.');
                        }

                        return $this->manager->value;
                    }
                };

                return new class($client) extends RedisConnection
                {
                    public function __construct(mixed $client)
                    {
                        $this->client = $client;
                    }

                    public function createSubscription($channels, \Closure $callback, $method = 'subscribe'): void {}
                };
            }

            public function purge(string $name): void
            {
                $this->purged[] = $name;
            }
        };

        try {
            $this->app->instance('redis', $manager);
            Redis::clearResolvedInstance('redis');

            $result = (new RedisStore('ambiguous-increment'))->increment('counter');

            $this->assertSame(2, $result);
            $this->assertSame(2, $manager->value);
            $this->assertSame(2, $manager->built);
            $this->assertSame(['ambiguous-increment'], $manager->purged);
        } finally {
            $this->app->instance('redis', $original);
            Redis::clearResolvedInstance('redis');
        }
    }

    public function test_schema_verification_rejects_misconfigured_model_primary_key_metadata(): void
    {
        $connection = DB::connection();
        $query = MisconfiguredPrimaryKeyPost::query()->toBase();
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve($connection, 'posts');

        $this->assertNotNull($table);
        $this->assertSame('missing_id', $query->primaryKey()?->column);
        $this->assertNull($this->app->make(PrimaryKeyResolver::class)->resolve(
            $query,
            $connection,
            $table,
        ));
    }

    public function test_fresh_schema_resolution_uses_two_gets_and_two_hash_reads(): void
    {
        $connection = DB::connection();
        $query = DB::table('posts');
        $warmRepository = $this->schemaRepository();
        $table = (new TableIdentityResolver($warmRepository))->resolve($connection, 'posts');

        $this->assertNotNull($table);
        $this->assertNotNull((new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $warmRepository,
        ))->resolve($query, $connection, $table));

        $commands = [];
        $redis = Redis::connection('normcache-test');
        $redis->setEventDispatcher($this->app->make('events'));
        $redis->listen(static function (CommandExecuted $event) use (&$commands): void {
            $commands[] = strtolower((string) $event->command);
        });

        $freshRepository = $this->schemaRepository();
        $freshTable = (new TableIdentityResolver($freshRepository))->resolve($connection, 'posts');

        $this->assertNotNull($freshTable);
        $this->assertNotNull((new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $freshRepository,
        ))->resolve($query, $connection, $freshTable));

        $metadataCommands = array_values(array_filter(
            $commands,
            static fn(string $command): bool => in_array($command, ['get', 'hget'], true),
        ));

        $this->assertSame(['get', 'get', 'hget', 'hget'], $metadataCommands);
    }

    /** @return array{0: Author, 1: Post} */
    private function createPost(string $title): array
    {
        $author = Author::query()->create(['name' => 'Author']);
        $post = Post::query()->create([
            'title' => $title,
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
        ]);

        return [$author, $post];
    }

    private function schemaRepository(): SchemaRepository
    {
        return new SchemaRepository(
            $this->app->make(CacheConfig::class),
            $this->app->make(RedisStore::class),
            $this->app->make(CacheKeyBuilder::class),
        );
    }
}
