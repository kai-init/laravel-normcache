<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\Fixtures\Models\VolatilePost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class RevalidationIntrospectionTest extends TestCase
{
    private int $authorId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->authorId = (int) Author::query()->create(['name' => 'Author'])->getKey();
    }

    public function test_a_precise_update_runs_only_the_update(): void
    {
        $this->configureRevalidation(true);
        $this->seedPosts(1);

        $queries = $this->captureQueries(
            fn() => RawPost::query()->toBase()->where('id', 1)->update(['title' => 'changed']),
        );

        $this->assertOnlyUpdateQuery($queries);
    }

    public function test_a_model_declaring_volatile_columns_runs_only_the_update(): void
    {
        $this->configureRevalidation(true);
        $this->seedPosts(1);

        $queries = $this->captureQueries(
            fn() => VolatilePost::query()->toBase()->where('id', 1)->update(['title' => 'changed']),
        );

        $this->assertOnlyUpdateQuery($queries);
    }

    public function test_a_broad_update_runs_only_the_update(): void
    {
        $this->configureRevalidation(true);
        $this->seedPosts(1);

        $queries = $this->captureQueries(
            fn() => RawPost::query()->toBase()->where('title', 'Post 1')->update(['views' => 2]),
        );

        $this->assertOnlyUpdateQuery($queries);
    }

    public function test_a_transactional_update_runs_no_catalog_query(): void
    {
        $this->configureRevalidation(true);
        $this->seedPosts(3);

        $catalog = [];
        DB::listen(function (QueryExecuted $query) use (&$catalog): void {
            foreach (['pragma_', 'sqlite_master', 'information_schema', 'pg_catalog', 'pg_trigger', '.sys.'] as $source) {
                if (str_contains(strtolower($query->sql), $source)) {
                    $catalog[] = $query->sql;
                }
            }
        });

        DB::transaction(function (): void {
            RawPost::query()->toBase()->where('id', 1)->update(['title' => 'first']);
            RawPost::query()->toBase()->where('id', 2)->update(['title' => 'second']);
        });

        $this->assertSame([], $catalog, 'invalidation must never inspect the catalog');
    }

    private function configureRevalidation(bool $enabled): void
    {
        config()->set('normcache.revalidation', $enabled);
        $this->app->forgetInstance(CacheConfig::class);
        $this->app->forgetScopedInstances();
    }

    private function seedPosts(int $count): void
    {
        $rows = [];

        for ($index = 1; $index <= $count; $index++) {
            $rows[] = [
                'title' => 'Post ' . $index,
                'views' => $index,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ];
        }

        RawPost::query()->toBase()->insert($rows);
    }

    /** @return list<array{query: string, bindings: array<mixed>, time: float}> */
    private function captureQueries(callable $callback): array
    {
        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            $callback();
        } finally {
            DB::disableQueryLog();
        }

        return DB::getQueryLog();
    }

    /** @param list<array{query: string, bindings: array<mixed>, time: float}> $queries */
    private function assertOnlyUpdateQuery(array $queries): void
    {
        $this->assertCount(1, $queries, 'invalidation should not issue a query of its own');
        $this->assertStringStartsWith('update ', strtolower($queries[0]['query']));
    }
}
