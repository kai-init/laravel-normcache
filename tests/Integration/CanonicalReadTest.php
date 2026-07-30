<?php

namespace NormCache\Tests\Integration;

use Closure;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\DB;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;
use ReflectionClass;

final class CanonicalReadTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) DB::table('posts')->insertGetId([
            'title' => 'Canonical',
            'views' => 10,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    public function test_canonical_list_populates_rows_reused_by_direct_pk_reads(): void
    {
        DB::table('posts')->orderBy('id')->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $row = DB::table('posts')->where('id', $this->postId)->first();
        DB::disableQueryLog();

        $this->assertSame('Canonical', $row->title);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_implicit_and_explicit_wildcards_share_the_canonical_cache_entry(): void
    {
        $expected = DB::table('posts')->orderBy('id')->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = DB::table('posts')->select('*')->orderBy('id')->get();
        DB::disableQueryLog();

        $this->assertSame(
            $expected->map(static fn(object $row): array => (array) $row)->all(),
            $actual->map(static fn(object $row): array => (array) $row)->all(),
        );
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_canonical_row_payload_must_match_the_primary_key_in_its_key(): void
    {
        $secondId = DB::table('posts')->insertGetId([
            'title' => 'Second',
            'views' => 20,
            'published' => true,
            'author_id' => DB::table('authors')->value('id'),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
        DB::table('posts')->where('id', $this->postId)->first();

        $query = DB::table('posts');
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve($query->getConnection(), $query->from);
        $this->assertNotNull($table);
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $epoch = $this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0';
        $rowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $this->postId);
        $second = DB::table('posts')->withoutCache()->where('id', $secondId)->first();
        $this->assertNotNull($second);
        $this->cacheStore()->setRaw(
            $rowKey,
            $this->app->make(RawResultCodec::class)->encodeRow($second, $epoch),
            60,
        );

        $row = DB::table('posts')->where('id', $this->postId)->first();

        $this->assertSame($this->postId, $row?->id);
        $this->assertSame('Canonical', $row?->title);
    }

    public function test_canonical_membership_tokens_must_match_the_primary_key_family(): void
    {
        $expected = DB::table('posts')->orderBy('id')->get();
        $this->deleteResultOverlays();
        $query = DB::table('posts');
        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve($query->getConnection(), $query->from);
        $this->assertNotNull($table);

        $membershipKey = $this->cacheKeysMatching(':m:v')[0] ?? null;
        $this->assertIsString($membershipKey);
        $membership = $this->app->make(MembershipCodec::class)
            ->decode((string) $this->cacheStore()->getRaw($membershipKey));
        $this->assertTrue($membership->valid);

        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $validRowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $this->postId);
        $invalidRowKey = $this->cacheKeys()->row($table, $generation, 's:' . $this->postId);
        $validRow = $this->cacheStore()->getRaw($validRowKey);
        $this->assertNotNull($validRow);
        $this->cacheStore()->setRaw($invalidRowKey, $validRow, 60);
        $this->cacheStore()->setRaw(
            $membershipKey,
            $this->app->make(MembershipCodec::class)->encode(
                $membership->epoch,
                $membership->generation,
                ['s:' . $this->postId],
                $membership->versions,
                $membership->tagVersion,
            ),
            60,
        );

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = DB::table('posts')->orderBy('id')->get();
        DB::disableQueryLog();

        $this->assertSame(
            $expected->map(static fn(object $row): array => (array) $row)->all(),
            $actual->map(static fn(object $row): array => (array) $row)->all(),
        );
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_sqlite_attached_schema_repairs_rows_from_the_qualified_table(): void
    {
        $path = sys_get_temp_dir() . '/normcache-attached-' . bin2hex(random_bytes(8)) . '.sqlite';
        touch($path);
        DB::statement('ATTACH DATABASE ? AS tenant', [$path]);

        try {
            DB::statement(<<<'SQL'
                CREATE TABLE tenant.posts (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    views INTEGER NOT NULL DEFAULT 0,
                    published INTEGER NOT NULL DEFAULT 1,
                    metadata TEXT NULL,
                    author_id INTEGER NOT NULL,
                    created_at TEXT NULL,
                    updated_at TEXT NULL,
                    deleted_at TEXT NULL
                )
                SQL);
            DB::table('tenant.posts')->insert([
                'id' => 1,
                'title' => 'Tenant',
                'views' => 20,
                'published' => true,
                'author_id' => 1,
                'created_at' => now(),
                'updated_at' => now(),
            ]);

            $read = fn() => DB::table('tenant.posts')->orderBy('id')->get();
            $expected = $read();
            $this->deleteResultOverlays();

            $identity = $this->app->make(TableIdentityResolver::class)
                ->resolve(DB::connection(), 'tenant.posts');
            $this->assertNotNull($identity);
            $generation = $this->cacheStore()->getRaw(
                $this->cacheKeys()->generation($identity),
            ) ?? '0';
            $rowKey = $this->cacheKeys()->row($identity, $generation, 'i:1');
            $this->assertNotNull($this->cacheStore()->getRaw($rowKey));
            $this->cacheStore()->delete($rowKey);

            DB::flushQueryLog();
            DB::enableQueryLog();
            $actual = $read();
            DB::disableQueryLog();

            $this->assertSame('Tenant', $expected->first()?->title);
            $this->assertSame('Tenant', $actual->first()?->title);
            $this->assertCount(1, DB::getQueryLog());
            $this->assertStringContainsString(
                '"tenant"."posts"',
                strtolower(DB::getQueryLog()[0]['query']),
            );
        } finally {
            $this->app->make(TableIdentityResolver::class)->clear('testing');
            DB::statement('DETACH DATABASE tenant');

            if (is_file($path)) {
                unlink($path);
            }
        }
    }

    public function test_large_canonical_query_is_published_without_an_admission_limit(): void
    {
        $timestamp = now();

        foreach (array_chunk(range(1, 1_000), 200) as $indexes) {
            DB::table('posts')->insert(array_map(
                static fn(int $index): array => [
                    'title' => "Post {$index}",
                    'views' => $index,
                    'published' => true,
                    'author_id' => DB::table('authors')->value('id'),
                    'created_at' => $timestamp,
                    'updated_at' => $timestamp,
                ],
                $indexes,
            ));
        }

        $rows = DB::table('posts')->orderBy('id')->get();

        $this->assertCount(1_001, $rows);
        $this->assertCount(1, $this->cacheKeysMatching(':m:v'));
        $this->assertCount(1_001, $this->cacheKeysMatching(':r:g'));
    }

    public function test_narrow_projection_uses_a_result_payload_without_widening_sql(): void
    {
        $cold = DB::table('posts')
            ->where('id', $this->postId)
            ->select('title as heading')
            ->first();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $warm = DB::table('posts')
            ->where('id', $this->postId)
            ->select('title as heading')
            ->first();
        DB::disableQueryLog();

        $this->assertSame(['heading' => 'Canonical'], (array) $cold);
        $this->assertSame((array) $cold, (array) $warm);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_missing_canonical_rows_are_repaired_by_primary_key_batch(): void
    {
        $expected = DB::table('posts')->orderBy('id')->get();
        $this->deleteResultOverlays();
        $rowKey = $this->cacheKeysMatching(':r:g')[0] ?? null;

        $this->assertIsString($rowKey);
        $this->cacheStore()->delete($rowKey);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $actual = DB::table('posts')->orderBy('id')->get();
        DB::disableQueryLog();

        $this->assertSame($expected->map(fn($row) => (array) $row)->all(), $actual->map(fn($row) => (array) $row)->all());
        $this->assertCount(1, DB::getQueryLog());
        $this->assertStringContainsString('where "id" in', strtolower(DB::getQueryLog()[0]['query']));
    }

    public function test_precise_invalidation_between_canonical_phases_cannot_mix_membership_and_row_versions(): void
    {
        $read = fn() => DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->get();

        $this->assertCount(1, $read());
        $this->assertCount(1, $read());
        $this->deleteResultOverlays();

        $postId = $this->postId;
        $store = $this->cacheStore();
        $storeReflection = new ReflectionClass($store);
        $connectionProperty = $storeReflection->getProperty('connection');
        $connection = $connectionProperty->getValue($store);

        $beforeFirstMget = function () use ($postId): void {
            DB::table('posts')->where('id', $postId)->update(['published' => false]);
            DB::table('posts')->where('id', $postId)->first();
        };

        if ($connection instanceof PhpRedisConnection) {
            $interceptingConnection = new class($connection->client(), $beforeFirstMget) extends PhpRedisConnection
            {
                private bool $intercepted = false;

                public function __construct(
                    mixed $client,
                    private Closure $beforeFirstMget,
                ) {
                    parent::__construct($client);
                }

                public function mget(array $keys): array
                {
                    if (!$this->intercepted) {
                        $this->intercepted = true;
                        ($this->beforeFirstMget)();
                    }

                    return parent::mget($keys);
                }
            };
        } else {
            $this->assertInstanceOf(PredisConnection::class, $connection);

            $interceptingConnection = new class($connection->client(), $beforeFirstMget) extends PredisConnection
            {
                private bool $intercepted = false;

                public function __construct(
                    mixed $client,
                    private Closure $beforeFirstMget,
                ) {
                    parent::__construct($client);
                }

                public function mget(array $keys): array
                {
                    if (!$this->intercepted) {
                        $this->intercepted = true;
                        ($this->beforeFirstMget)();
                    }

                    return parent::__call('mget', [$keys]);
                }
            };
        }
        $connectionProperty->setValue($store, $interceptingConnection);

        try {
            $rows = $read();
        } finally {
            $connectionProperty->setValue($store, $connection);
        }

        $currentRows = DB::table('posts')
            ->where('published', true)
            ->orderBy('id')
            ->withoutCache()
            ->get();

        $this->assertCount(0, $currentRows);
        $this->assertCount(0, $rows);
    }

    public function test_direct_primary_key_rows_apply_builtin_soft_delete_visibility(): void
    {
        Post::query()->whereKey($this->postId)->delete();
        $trashed = Post::withTrashed()->findOrFail($this->postId);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $default = Post::query()->find($this->postId);
        $only = Post::onlyTrashed()->find($this->postId);
        DB::disableQueryLog();

        $this->assertNull($default);
        $this->assertSame($trashed->getKey(), $only?->getKey());
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_or_deleted_at_predicate_is_not_misclassified_as_a_direct_lookup(): void
    {
        $second = Post::query()->create([
            'title' => 'Second',
            'views' => 0,
            'published' => true,
            'author_id' => DB::table('authors')->value('id'),
        ]);

        $read = fn() => Post::withTrashed()
            ->whereKey($this->postId)
            ->orWhereNull('deleted_at')
            ->get();

        $this->assertCount(2, $read());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $ids = $read()->modelKeys();
        DB::disableQueryLog();

        $this->assertContains($this->postId, $ids);
        $this->assertContains($second->getKey(), $ids);
        $this->assertSame([], DB::getQueryLog());
    }

    public function test_string_primary_keys_share_canonical_rows_with_direct_reads(): void
    {
        $id = 'uuid:with/{delimiters}';
        UuidItem::query()->create(['id' => $id, 'name' => 'String key']);
        UuidItem::query()->get();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $item = UuidItem::query()->findOrFail($id);
        DB::disableQueryLog();

        $this->assertSame('String key', $item->name);
        $this->assertSame([], DB::getQueryLog());
    }
}
