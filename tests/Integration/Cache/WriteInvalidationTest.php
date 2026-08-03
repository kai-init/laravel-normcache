<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Query\Builder as LaravelQueryBuilder;
use Illuminate\Database\Query\Grammars\PostgresGrammar;
use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use NormCache\Database\Connections\BuildsCachingQueries;
use NormCache\Database\QueryBuilder;
use NormCache\Events\CacheInvalidated;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;

final class AppliedThenThrowsConnection extends SQLiteConnection
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

final class WriteInvalidationTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Before',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_db_table_update_invalidates_warm_results(): void
    {
        $read = fn() => DB::table('posts')->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        DB::table('posts')->where('id', $this->postId)->update(['title' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('After', $read());
        $this->assertSame('After', $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_simple_raw_write_target_invalidates_warm_results(): void
    {
        $read = fn() => DB::table('posts')->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        DB::table(DB::raw('posts'))
            ->where('id', $this->postId)
            ->update(['title' => 'Raw target']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Raw target', $read());
        $this->assertSame('Raw target', $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_opaque_write_target_advances_the_global_epoch(): void
    {
        DB::table('posts')->where('id', $this->postId)->first();
        $before = (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');

        DB::table(DB::raw('posts NOT INDEXED'))
            ->where('id', $this->postId)
            ->update(['title' => 'Opaque target']);

        $this->assertSame(
            $before + 1,
            (int) $this->cacheStore()->getRaw($this->cacheKeys()->epoch()),
        );
        $this->assertSame(
            'Opaque target',
            DB::table('posts')->where('id', $this->postId)->value('title'),
        );
    }

    public function test_opaque_write_global_invalidation_waits_for_commit(): void
    {
        $before = (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');

        DB::beginTransaction();
        DB::table(DB::raw('posts NOT INDEXED'))
            ->where('id', $this->postId)
            ->update(['title' => 'Committed opaque']);
        $this->assertSame(
            $before,
            (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0'),
        );
        DB::commit();

        $this->assertSame(
            $before + 1,
            (int) $this->cacheStore()->getRaw($this->cacheKeys()->epoch()),
        );
    }

    public function test_opaque_write_global_invalidation_is_discarded_on_rollback(): void
    {
        $before = $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0';

        DB::beginTransaction();
        DB::table(DB::raw('posts NOT INDEXED'))
            ->where('id', $this->postId)
            ->update(['title' => 'Rolled back opaque']);
        DB::rollBack();

        $this->assertSame(
            $before,
            $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0',
        );
    }

    public function test_applied_write_that_throws_invalidates_before_rethrowing(): void
    {
        $name = 'uncertain-write';
        $database = (string) DB::connection()->getDatabaseName();

        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
            'name' => $name,
            'normcache_scope' => $name,
        ]);
        DB::extend($name, static fn(array $config) => new AppliedThenThrowsConnection(
            new \PDO('sqlite:' . $database),
            $database,
            '',
            $config,
        ));
        DB::purge($name);

        try {
            $connection = DB::connection($name);
            $this->assertInstanceOf(AppliedThenThrowsConnection::class, $connection);
            $postId = $this->postId;
            $read = static fn() => $connection
                ->table('posts')
                ->where('id', $postId)
                ->value('title');

            $this->assertSame('Before', $read());
            $this->assertSame('Before', $read());
            $connection->throwAfterNextAffectingStatement = true;

            try {
                $connection
                    ->table('posts')
                    ->where('id', $this->postId)
                    ->update(['title' => 'Applied then thrown']);
                $this->fail('The simulated lost response was not thrown.');
            } catch (\RuntimeException $exception) {
                $this->assertSame(
                    'The database applied the write but the response was lost.',
                    $exception->getMessage(),
                );
            }

            $this->assertSame('Applied then thrown', $read());
            $connection->throwAfterNextAffectingStatement = true;

            try {
                $connection
                    ->table('posts')
                    ->updateOrInsert(
                        ['id' => $postId],
                        ['title' => 'Nested applied then thrown'],
                    );
                $this->fail('The nested simulated lost response was not thrown.');
            } catch (\RuntimeException $exception) {
                $this->assertSame(
                    'The database applied the write but the response was lost.',
                    $exception->getMessage(),
                );
            }

            $this->assertSame('Nested applied then thrown', $read());
        } finally {
            DB::disconnect($name);
            DB::purge($name);
            DB::forgetExtension($name);
        }
    }

    public function test_sqlite_identifier_case_variants_share_invalidation_state(): void
    {
        $read = fn() => DB::table('posts')
            ->where('id', $this->postId)
            ->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        DB::table('POSTS')->where('id', $this->postId)->update(['title' => 'Case-safe']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $read();
        DB::disableQueryLog();

        $this->assertSame('Case-safe', $actual);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_traitless_eloquent_writes_invalidate_opted_in_reads(): void
    {
        $this->assertSame('Before', Post::query()->findOrFail($this->postId)->title);

        UncachedPost::query()->whereKey($this->postId)->update(['title' => 'Traitless']);

        $this->assertSame('Traitless', Post::query()->findOrFail($this->postId)->title);
    }

    public function test_transaction_writes_publish_no_invalidation_before_commit(): void
    {
        DB::table('posts')->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        DB::beginTransaction();
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Committed']);
        $this->assertSame($before, $this->tableVersion());
        DB::commit();

        $this->assertSame((string) ((int) $before + 1), $this->tableVersion());
    }

    public function test_outer_transaction_rollback_discards_pending_invalidation(): void
    {
        DB::table('posts')->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        DB::beginTransaction();
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Rolled back']);
        DB::rollBack();

        $this->assertSame($before, $this->tableVersion());
        $this->assertSame(
            'Before',
            DB::table('posts')->where('id', $this->postId)->value('title'),
        );
    }

    public function test_update_or_insert_internal_exists_is_live_and_invalidates_once(): void
    {
        DB::table('posts')->where('id', $this->postId)->exists();
        $before = (int) $this->tableVersion();

        DB::table('posts')->updateOrInsert(
            ['id' => $this->postId],
            ['title' => 'Composite'],
        );

        $this->assertSame($before + 1, (int) $this->tableVersion());
        $this->assertSame('Composite', DB::table('posts')->where('id', $this->postId)->value('title'));
    }

    public function test_proven_primary_key_update_preserves_unrelated_canonical_rows(): void
    {
        $second = DB::table('posts')->insertGetId([
            'title' => 'Second',
            'views' => 0,
            'published' => true,
            'author_id' => DB::table('authors')->value('id'),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
        DB::table('posts')->orderBy('id')->get();

        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generationBefore = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';
        $secondRowKey = $keys->row($identity, $generationBefore, 'i:' . $second);

        $this->assertNotNull($this->cacheStore()->getRaw($secondRowKey));

        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Precise']);

        $this->assertSame(
            $generationBefore,
            $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0',
        );
        $this->assertNotNull($this->cacheStore()->getRaw($secondRowKey));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $second)->first();
        DB::disableQueryLog();

        $this->assertSame('Second', $row->title);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_eloquent_joined_update_on_related_primary_key_uses_broad_invalidation(): void
    {
        $second = Post::query()->create([
            'title' => 'Second',
            'views' => 0,
            'published' => true,
            'author_id' => Author::query()->value('id'),
        ]);
        Post::query()->orderBy('id')->get();

        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generationBefore = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';

        Post::query()
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->where('authors.id', Author::query()->value('id'))
            ->update(['title' => 'Joined']);

        $this->assertSame(
            (string) ((int) $generationBefore + 1),
            $this->cacheStore()->getRaw($keys->generation($identity)),
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $reloaded = Post::query()->findOrFail($second->getKey());
        DB::disableQueryLog();

        $this->assertSame('Joined', $reloaded->title);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_primary_key_mutation_deletes_the_cached_old_token_without_advancing_generation(): void
    {
        DB::table('posts')->orderBy('id')->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generation = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';
        $version = $this->cacheStore()->getRaw($keys->version($identity)) ?? '0';
        $oldRow = $keys->row($identity, $generation, 'i:' . $this->postId);
        $newId = $this->postId + 1000;

        DB::table('posts')
            ->where('id', $this->postId)
            ->update(['id' => $newId]);

        $this->assertNull($this->cacheStore()->getRaw($oldRow));
        $this->assertSame(
            (string) ((int) $version + 1),
            $this->cacheStore()->getRaw($keys->version($identity)),
        );
        $this->assertSame(
            $generation,
            $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0',
        );
    }

    public function test_unprovable_primary_key_mutation_promotes_to_generation_invalidation(): void
    {
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $before = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';

        DB::table('posts')
            ->where('id', $this->postId)
            ->update(['id' => DB::raw('id + 1000')]);

        $this->assertSame(
            (string) ((int) $before + 1),
            $this->cacheStore()->getRaw($keys->generation($identity)),
        );
    }

    public function test_string_primary_key_mutations_use_generation_invalidation(): void
    {
        UuidItem::query()->create(['id' => 'abc', 'name' => 'Before']);
        UuidItem::query()->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'uuid_items');
        $keys = $this->cacheKeys();
        $before = $this->cacheStore()->getRaw(
            $keys->generation($identity),
        ) ?? '0';

        UuidItem::query()->whereKey('abc')->update(['name' => 'After']);

        $this->assertSame(
            (string) ((int) $before + 1),
            $this->cacheStore()->getRaw($keys->generation($identity)),
        );
    }

    public function test_insert_get_id_preserves_string_processor_results(): void
    {
        $connection = DB::connection();
        $processor = new class extends Processor
        {
            public function processInsertGetId($query, $sql, $values, $sequence = null)
            {
                return 'generated-uuid';
            }
        };
        $builder = new QueryBuilder(
            $connection,
            $connection->getQueryGrammar(),
            $processor,
        );
        $builder->from('uuid_items');

        $this->assertSame(
            'generated-uuid',
            $builder->insertGetId(['name' => 'Ignored by fake processor']),
        );
    }

    public function test_precise_invalidation_deletes_the_atomically_resolved_current_generation(): void
    {
        $this->cacheManager()->invalidateTable('testing', 'posts');
        DB::table('posts')->orderBy('id')->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generation = $this->cacheStore()->getRaw($keys->generation($identity));
        $rowKey = $keys->row($identity, (string) $generation, 'i:' . $this->postId);

        $this->assertNotNull($this->cacheStore()->getRaw($rowKey));
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Current generation']);
        $this->assertNull($this->cacheStore()->getRaw($rowKey));
    }

    public function test_truncate_broadly_invalidates_all_cached_rows(): void
    {
        $first = Tag::create(['name' => 'First']);
        $second = Tag::create(['name' => 'Second']);

        $this->assertCount(2, Tag::orderBy('id')->get());
        $this->assertNotNull(Tag::find($first->getKey()));
        $this->assertNotNull(Tag::find($second->getKey()));
        Event::fake([CacheInvalidated::class]);

        DB::table('tags')->truncate();

        $this->assertSame([], Tag::orderBy('id')->get()->all());
        $this->assertNull(Tag::find($first->getKey()));
        $this->assertNull(Tag::find($second->getKey()));
        Event::assertDispatched(
            CacheInvalidated::class,
            fn(CacheInvalidated $event): bool => $event->mode === 'generation',
        );
    }

    public function test_insert_using_invalidates_the_target_table(): void
    {
        $source = Author::create(['name' => 'Copied']);
        $read = fn(): array => Tag::orderBy('name')->pluck('name')->all();

        $this->assertSame([], $read());
        $this->assertSame([], $read());

        $affected = DB::table('tags')->insertUsing(
            ['name', 'created_at', 'updated_at'],
            DB::table('authors')
                ->where('id', $source->getKey())
                ->select(['name', 'created_at', 'updated_at']),
        );

        $this->assertSame(1, $affected);
        $this->assertSame(['Copied'], $read());
    }

    public function test_insert_or_ignore_using_invalidates_the_target_table(): void
    {
        $source = Author::create(['name' => 'Copied']);
        $read = fn(): array => UuidItem::orderBy('id')->pluck('name', 'id')->all();

        $this->assertSame([], $read());
        $this->assertSame([], $read());

        $affected = DB::table('uuid_items')->insertOrIgnoreUsing(
            ['id', 'name'],
            DB::table('authors')
                ->where('id', $source->getKey())
                ->select(['id', 'name']),
        );

        $this->assertSame(1, $affected);
        $this->assertSame([$source->getKey() => 'Copied'], $read());
    }

    public function test_insert_or_ignore_returning_invalidates_the_target_table(): void
    {
        if (!method_exists(LaravelQueryBuilder::class, 'insertOrIgnoreReturning')) {
            $this->markTestSkipped('insertOrIgnoreReturning requires this Laravel version.');
        }

        $read = fn(): array => Tag::orderBy('id')->pluck('name', 'id')->all();

        $this->assertSame([], $read());
        $this->assertSame([], $read());

        $returned = DB::table('tags')->insertOrIgnoreReturning([
            'id' => 10,
            'name' => 'Returned',
            'created_at' => now(),
            'updated_at' => now(),
        ], ['id', 'name']);

        $this->assertCount(1, $returned);
        $this->assertSame(10, $returned->first()->id);
        $this->assertSame([10 => 'Returned'], $read());
    }

    public function test_update_from_invalidates_the_target_table(): void
    {
        $author = Author::create(['name' => 'Before']);
        $read = fn(): string => Author::whereKey($author->getKey())->value('name');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        $connection = DB::connection();
        $builder = new QueryBuilder(
            $connection,
            new PostgresGrammar($connection),
            $connection->getPostProcessor(),
        );
        $builder->from('authors')->where('id', $author->getKey());

        $this->assertSame(1, $builder->updateFrom(['name' => 'After']));
        $this->assertSame('After', $read());
    }

    private function tableVersion(): string
    {
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');

        return $this->cacheStore()->getRaw(
            $this->cacheKeys()->version($identity),
        ) ?? '0';
    }
}
