<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Query\Builder as LaravelQueryBuilder;
use Illuminate\Database\Query\Grammars\PostgresGrammar;
use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\DB;
use NormCache\Database\QueryBuilder;
use NormCache\Facades\NormCache;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Fixtures\Models\UncachedPost;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;
use NormCache\Values\PrimaryKeyMetadata;

final class AppliedThenThrowsConnection extends SQLiteConnection
{
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
        $this->postId = (int) RawPost::query()->toBase()->insertGetId([
            'title' => 'Before',
            'views' => 0,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_cacheable_base_builder_update_invalidates_warm_results(): void
    {
        $read = fn() => RawPost::query()->toBase()->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'After']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('After', $read());
        $this->assertSame('After', $read());
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_db_table_write_remains_stale_until_explicit_invalidation(): void
    {
        $read = fn() => RawPost::query()->toBase()->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        DB::table('posts')
            ->where('id', $this->postId)
            ->update(['title' => 'Manual']);

        $this->assertSame('Before', $read());
        $this->assertTrue(NormCache::invalidate(Post::class));
        $this->assertSame('Manual', $read());
    }

    public function test_manual_invalidation_is_immediate_outside_a_transaction(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        $this->assertTrue(NormCache::invalidate(Post::class));

        $this->assertSame((string) ((int) $before + 1), $this->tableVersion());
    }

    public function test_manual_invalidation_inside_a_transaction_waits_for_commit(): void
    {
        $read = fn() => RawPost::query()->toBase()->where('id', $this->postId)->value('title');
        $this->assertSame('Before', $read());
        $before = $this->tableVersion();

        DB::beginTransaction();
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Committed manual']);
        $this->assertTrue(NormCache::invalidate(Post::class));
        $this->assertSame($before, $this->tableVersion());
        DB::commit();

        $this->assertSame((string) ((int) $before + 1), $this->tableVersion());
        $this->assertSame('Committed manual', $read());
    }

    public function test_manual_invalidation_inside_a_rolled_back_transaction_is_discarded(): void
    {
        $read = fn() => RawPost::query()->toBase()->where('id', $this->postId)->value('title');
        $this->assertSame('Before', $read());
        $before = $this->tableVersion();

        DB::beginTransaction();
        DB::table('posts')->where('id', $this->postId)->update(['title' => 'Rolled back manual']);
        $this->assertTrue(NormCache::invalidate(Post::class));
        DB::rollBack();

        $this->assertSame($before, $this->tableVersion());
        $this->assertSame('Before', $read());
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
            $read = static fn() => RawPost::on($name)
                ->toBase()
                ->where('id', $postId)
                ->value('title');

            $this->assertSame('Before', $read());
            $this->assertSame('Before', $read());
            $connection->throwAfterNextAffectingStatement = true;

            try {
                RawPost::on($name)
                    ->toBase()
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
                RawPost::on($name)
                    ->toBase()
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
        $read = fn() => RawPost::query()->toBase()
            ->where('id', $this->postId)
            ->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());

        RawPost::query()->toBase()->from('POSTS')->where('id', $this->postId)->update(['title' => 'Case-safe']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = $read();
        DB::disableQueryLog();

        $this->assertSame('Case-safe', $actual);
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_traitless_eloquent_write_remains_stale_until_explicit_invalidation(): void
    {
        $this->assertSame('Before', Post::query()->findOrFail($this->postId)->title);
        $this->assertSame('Before', Post::query()->findOrFail($this->postId)->title);

        UncachedPost::query()->whereKey($this->postId)->update(['title' => 'Traitless']);

        $this->assertSame('Before', Post::query()->findOrFail($this->postId)->title);
        $this->assertTrue(NormCache::invalidate(Post::class));
        $this->assertSame('Traitless', Post::query()->findOrFail($this->postId)->title);
    }

    public function test_transaction_writes_publish_no_invalidation_before_commit(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        DB::beginTransaction();
        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Committed']);
        $this->assertSame($before, $this->tableVersion());
        DB::commit();

        $this->assertSame((string) ((int) $before + 1), $this->tableVersion());
    }

    public function test_outer_transaction_rollback_discards_pending_invalidation(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        DB::beginTransaction();
        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Rolled back']);
        DB::rollBack();

        $this->assertSame($before, $this->tableVersion());
        $this->assertSame(
            'Before',
            RawPost::query()->toBase()->where('id', $this->postId)->value('title'),
        );
    }

    public function test_transaction_invalidation_is_published_before_after_commit_callbacks(): void
    {
        $read = fn(): ?string => RawPost::query()->toBase()->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());
        $observed = null;

        DB::transaction(function () use (&$observed, $read): void {
            RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Committed']);
            DB::afterCommit(function () use (&$observed, $read): void {
                $observed = $read();
            });
        });

        $this->assertSame('Committed', $observed);
    }

    public function test_transaction_invalidation_precedes_a_callback_registered_before_the_write(): void
    {
        $read = fn(): ?string => RawPost::query()->toBase()->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());
        $observed = null;

        DB::transaction(function () use (&$observed, $read): void {
            DB::afterCommit(function () use (&$observed, $read): void {
                $observed = $read();
            });
            RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Committed']);
        });

        $this->assertSame('Committed', $observed);
    }

    public function test_transaction_invalidation_precedes_a_callback_registered_in_a_nested_transaction(): void
    {
        $read = fn(): ?string => RawPost::query()->toBase()->where('id', $this->postId)->value('title');

        $this->assertSame('Before', $read());
        $this->assertSame('Before', $read());
        $observed = null;

        DB::transaction(function () use (&$observed, $read): void {
            DB::transaction(function () use (&$observed, $read): void {
                RawPost::query()->toBase()
                    ->where('id', $this->postId)
                    ->update(['title' => 'Committed']);

                DB::afterCommit(function () use (&$observed, $read): void {
                    $observed = $read();
                });
            });
        });

        $this->assertSame('Committed', $observed);
    }

    public function test_update_or_insert_internal_exists_is_live_and_invalidates_once(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->exists();
        $before = (int) $this->tableVersion();

        RawPost::query()->toBase()->updateOrInsert(
            ['id' => $this->postId],
            ['title' => 'Composite'],
        );

        $this->assertSame($before + 1, (int) $this->tableVersion());
        $this->assertSame('Composite', RawPost::query()->toBase()->where('id', $this->postId)->value('title'));
    }

    public function test_update_or_insert_existing_row_without_values_does_not_invalidate(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        $this->assertTrue(RawPost::query()->toBase()->updateOrInsert(['id' => $this->postId]));

        $this->assertSame($before, $this->tableVersion());
    }

    public function test_update_or_insert_missing_row_without_values_still_invalidates(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = (int) $this->tableVersion();

        $this->assertTrue(RawPost::query()->toBase()->updateOrInsert([
            'id' => $this->postId + 1,
            'title' => 'Inserted',
            'author_id' => Author::query()->value('id'),
            'created_at' => now(),
            'updated_at' => now(),
        ]));

        $this->assertSame($before + 1, (int) $this->tableVersion());
    }

    public function test_update_matching_no_rows_does_not_invalidate(): void
    {
        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = $this->tableVersion();

        $this->assertSame(0, RawPost::query()->toBase()->where('id', -1)->update(['title' => 'Nobody']));

        $this->assertSame($before, $this->tableVersion());
    }

    public function test_update_or_insert_invalidates_exactly_when_its_nested_update_reports_rows(): void
    {
        $values = ['title' => 'Unchanged'];
        RawPost::query()->toBase()->where('id', $this->postId)->update($values);

        // Drivers disagree about a value-preserving update: MySQL reports zero
        // affected rows, SQLite and Postgres report the matched row. Probe this
        // one so the expectation follows the driver rather than assuming one.
        $affected = RawPost::query()->toBase()->where('id', $this->postId)->update($values);

        RawPost::query()->toBase()->where('id', $this->postId)->get();
        $before = (int) $this->tableVersion();

        $this->assertSame(
            $affected > 0,
            RawPost::query()->toBase()->updateOrInsert(['id' => $this->postId], $values),
        );
        $this->assertSame(
            $before + ($affected > 0 ? 1 : 0),
            (int) $this->tableVersion(),
            'updateOrInsert must invalidate on the same terms as the update it delegates to',
        );
    }

    public function test_proven_primary_key_update_preserves_unrelated_canonical_rows(): void
    {
        $second = RawPost::query()->toBase()->insertGetId([
            'title' => 'Second',
            'views' => 0,
            'published' => true,
            'author_id' => Author::query()->toBase()->value('id'),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
        RawPost::query()->toBase()->orderBy('id')->get();

        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generationBefore = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';
        $secondRowKey = $keys->row($identity, $generationBefore, 'i:' . $second);

        $this->assertNotNull($this->cacheStore()->getRaw($secondRowKey));

        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Precise']);

        $this->assertSame(
            $generationBefore,
            $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0',
        );
        $this->assertNotNull($this->cacheStore()->getRaw($secondRowKey));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = RawPost::query()->toBase()->where('id', $second)->first();
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
        RawPost::query()->toBase()->orderBy('id')->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generation = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';
        $version = $this->cacheStore()->getRaw($keys->version($identity)) ?? '0';
        $oldRow = $keys->row($identity, $generation, 'i:' . $this->postId);
        $newId = $this->postId + 1000;

        RawPost::query()->toBase()
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

        RawPost::query()->toBase()
            ->where('id', $this->postId)
            ->update(['id' => DB::raw('id + 1000')]);

        $this->assertSame(
            (string) ((int) $before + 1),
            $this->cacheStore()->getRaw($keys->generation($identity)),
        );
    }

    public function test_string_primary_key_mutations_invalidate_broadly(): void
    {
        UuidItem::query()->create(['id' => 'abc', 'name' => 'Before']);
        UuidItem::query()->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'uuid_items');
        $keys = $this->cacheKeys();
        $generation = $this->cacheStore()->getRaw($keys->generation($identity)) ?? '0';
        $version = $this->cacheStore()->getRaw($keys->version($identity)) ?? '0';
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::STRING);
        $token = $primaryKey->token('abc');

        $this->assertNotNull($token);
        $row = $keys->row($identity, $generation, $token);
        $this->assertNotNull($this->cacheStore()->getRaw($row));

        UuidItem::query()->whereKey('abc')->update(['name' => 'After']);

        $this->assertSame(
            (string) ((int) $generation + 1),
            $this->cacheStore()->getRaw($keys->generation($identity)),
            'string keys compare case-insensitively under common collations, so a row token is not proof of row identity',
        );
        $this->assertSame(
            (string) ((int) $version + 1),
            $this->cacheStore()->getRaw($keys->version($identity)),
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
        $builder->enableCachingForModel(UuidItem::class, 'id', 'string')
            ->from('uuid_items');

        $this->assertSame(
            'generated-uuid',
            $builder->insertGetId(['name' => 'Ignored by fake processor']),
        );
    }

    public function test_precise_invalidation_deletes_the_atomically_resolved_current_generation(): void
    {
        $this->cacheManager()->invalidateTable('testing', 'posts');
        RawPost::query()->toBase()->orderBy('id')->get();
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $keys = $this->cacheKeys();
        $generation = $this->cacheStore()->getRaw($keys->generation($identity));
        $rowKey = $keys->row($identity, (string) $generation, 'i:' . $this->postId);

        $this->assertNotNull($this->cacheStore()->getRaw($rowKey));
        RawPost::query()->toBase()->where('id', $this->postId)->update(['title' => 'Current generation']);
        $this->assertNull($this->cacheStore()->getRaw($rowKey));
    }

    public function test_truncate_broadly_invalidates_all_rows_in_its_table(): void
    {
        $first = Tag::create(['name' => 'First']);
        $second = Tag::create(['name' => 'Second']);

        $this->assertCount(2, Tag::orderBy('id')->get());
        $this->assertNotNull(Tag::find($first->getKey()));
        $this->assertNotNull(Tag::find($second->getKey()));
        $identity = app(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'tags');
        $generation = $this->cacheStore()->getRaw(
            $this->cacheKeys()->generation($identity),
        ) ?? '0';
        $epoch = (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');

        Tag::query()->toBase()->truncate();

        $this->assertSame(
            $epoch,
            (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0'),
        );
        $this->assertSame(
            (string) ((int) $generation + 1),
            $this->cacheStore()->getRaw($this->cacheKeys()->generation($identity)),
        );
        $this->assertSame([], Tag::orderBy('id')->get()->all());
        $this->assertNull(Tag::find($first->getKey()));
        $this->assertNull(Tag::find($second->getKey()));
    }

    public function test_insert_using_invalidates_the_target_table(): void
    {
        $source = Author::create(['name' => 'Copied']);
        $read = fn(): array => Tag::orderBy('name')->pluck('name')->all();

        $this->assertSame([], $read());
        $this->assertSame([], $read());

        $affected = Tag::query()->toBase()->insertUsing(
            ['name', 'created_at', 'updated_at'],
            Author::query()->toBase()
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

        $affected = UuidItem::query()->toBase()->insertOrIgnoreUsing(
            ['id', 'name'],
            Author::query()->toBase()
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

        $returned = Tag::query()->toBase()->insertOrIgnoreReturning([
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
        $builder->enableCachingForModel(Author::class, 'id', 'int')
            ->from('authors')
            ->where('id', $author->getKey());

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
