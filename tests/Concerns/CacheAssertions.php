<?php

namespace NormCache\Tests\Concerns;

use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Pagination\CursorPaginator;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use Predis\Client;

trait CacheAssertions
{
    /** @return list<string> */
    protected function cacheKeysMatching(string $needle): array
    {
        $connection = Redis::connection('normcache-test');
        $client = $connection->client();
        $keys = [];

        if ($client instanceof \RedisCluster) {
            $raw = $client->keys('*');
            $keys = is_array($raw) ? array_merge([], ...array_map(static fn(mixed $v): array => (array) $v, $raw)) : [];
        } elseif (class_exists(Client::class) && $client instanceof Client && $this->isClusterRun()) {
            foreach ($client as $node) {
                $keys = [...$keys, ...(array) $node->keys('*')];
            }
        } else {
            $keys = (array) $connection->keys('*');
        }

        return array_values(array_filter(
            array_unique(array_map(strval(...), $keys)),
            static fn(string $key): bool => str_contains($key, $needle),
        ));
    }

    protected function deleteResultOverlays(): void
    {
        $this->cacheStore()->delete($this->cacheKeysMatching(':e:v'));
    }

    private function isClusterRun(): bool
    {
        return env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true;
    }

    /** @return list<array<string, mixed>> */
    protected function contract(
        callable $cached,
        callable $native,
        bool $expectNoStrayQueries = true,
        ?callable $mutate = null,
    ): array {
        $this->cacheManager()->flushAll();
        $expected = $this->nativeResult($native);
        [$cold] = $this->observedCacheCall($cached, 'miss');
        [$warm, $strayQueries] = $this->observedCacheCall($cached, 'hit', captureQueries: true);

        $this->assertSame($expected, $cold, 'cold cache result differs from native Eloquent');
        $this->assertSame($cold, $warm, 'warm cache result differs from cold');

        if ($expectNoStrayQueries) {
            $this->assertSame([], $strayQueries, 'expected the warm result to be served entirely from NormCache');
        }

        if ($mutate !== null) {
            $mutate();
            $expectedAfterMutation = $this->nativeResult($native);
            [$fresh] = $this->observedCacheCall($cached, 'miss');
            [$rewarmed, $rewarmedQueries] = $this->observedCacheCall(
                $cached,
                'hit',
                captureQueries: true,
            );

            $this->assertSame(
                $expectedAfterMutation,
                $fresh,
                'first read after dependency mutation differs from native Eloquent',
            );
            $this->assertSame($fresh, $rewarmed, 'rewarmed result differs from refreshed result');

            if ($expectNoStrayQueries) {
                $this->assertSame(
                    [],
                    $rewarmedQueries,
                    'expected the rewarmed result to be served entirely from NormCache',
                );
            }
        }

        return $strayQueries;
    }

    protected function assertWarmCacheHit(callable $query): void
    {
        [, $queries] = $this->observedCacheCall($query, 'hit', captureQueries: true);
        $this->assertSame([], $queries, 'expected the query to be served entirely from NormCache');
    }

    protected function assertColdCacheMiss(callable $query): void
    {
        $this->observedCacheCall($query, 'miss');
    }

    /** @return list<array<string, mixed>> */
    protected function missContract(callable $query, callable $native): array
    {
        $this->cacheManager()->flushAll();
        $expected = $this->nativeResult($native);
        [$first] = $this->observedCacheCall($query, 'miss', captureQueries: true);
        [$second, $queries] = $this->observedCacheCall($query, 'miss', captureQueries: true);

        $this->assertSame($expected, $first, 'uncached result differs from native Eloquent');
        $this->assertSame($first, $second, 'repeated uncached result differs from the first execution');
        $this->assertNotSame([], $queries, 'expected an uncached operation to execute SQL');

        return $queries;
    }

    /** @return list<array<string, mixed>> */
    protected function databaseContract(callable $query, callable $native): array
    {
        $this->cacheManager()->flushAll();
        $expected = $this->nativeResult($native);
        [$first] = $this->observedCacheCall($query, 'none', captureQueries: true);
        [$second, $queries] = $this->observedCacheCall($query, 'none', captureQueries: true);

        $this->assertSame($expected, $first, 'direct database result differs from native Eloquent');
        $this->assertSame($first, $second, 'repeated direct database result differs from the first execution');
        $this->assertNotSame([], $queries, 'expected a direct database operation to execute SQL');

        return $queries;
    }

    /** @return list<array<string, mixed>> */
    protected function bypassContract(
        callable $query,
        callable $native,
        ?string $reason = null,
    ): array {
        $this->cacheManager()->flushAll();
        $expected = $this->nativeResult($native);
        [$first] = $this->observedCacheCall($query, 'bypass', captureQueries: true, bypassReason: $reason);
        [$second, $queries] = $this->observedCacheCall(
            $query,
            'bypass',
            captureQueries: true,
            bypassReason: $reason,
        );

        $this->assertSame($expected, $first, 'bypassed result differs from native Eloquent');
        $this->assertSame($first, $second, 'repeated bypass result differs from the first execution');
        $this->assertNotSame([], $queries, 'expected a bypassed operation to execute SQL');

        return $queries;
    }

    protected function nativeResult(callable $query): mixed
    {
        return $this->normalize($this->cacheManager()->withoutCache($query));
    }

    /**
     * @return array{0: mixed, 1: list<array<string, mixed>>}
     */
    private function observedCacheCall(
        callable $callback,
        string $expectedOutcome,
        bool $captureQueries = false,
        ?string $bypassReason = null,
    ): array {
        $dispatcher = Event::getFacadeRoot();
        Event::fake([
            QueryCacheHit::class,
            QueryCacheMiss::class,
            QueryBypassed::class,
        ]);

        if ($captureQueries) {
            DB::flushQueryLog();
            DB::enableQueryLog();
        }

        try {
            $result = $this->normalize($callback());
            $queries = $captureQueries ? DB::getQueryLog() : [];

            match ($expectedOutcome) {
                'hit' => $this->assertCacheHitEvents(),
                'miss' => $this->assertCacheMissEvents(),
                'bypass' => $this->assertCacheBypassEvents($bypassReason),
                'none' => $this->assertNoCacheEvents(),
                default => throw new \InvalidArgumentException("Unknown cache contract outcome [{$expectedOutcome}]."),
            };

            return [$result, $queries];
        } finally {
            if ($captureQueries) {
                DB::disableQueryLog();
            }

            Event::swap($dispatcher);
        }
    }

    private function assertCacheHitEvents(): void
    {
        Event::assertDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryBypassed::class);
    }

    private function assertCacheMissEvents(): void
    {
        Event::assertDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryBypassed::class);
    }

    private function assertCacheBypassEvents(?string $reason): void
    {
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
        Event::assertDispatched(
            QueryBypassed::class,
            $reason === null
                ? null
                : static fn(QueryBypassed $event): bool => $event->reason === $reason,
        );
    }

    private function assertNoCacheEvents(): void
    {
        Event::assertNotDispatched(QueryCacheHit::class);
        Event::assertNotDispatched(QueryCacheMiss::class);
        Event::assertNotDispatched(QueryBypassed::class);
    }

    protected function normalize(mixed $value): mixed
    {
        if ($value instanceof LengthAwarePaginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'total' => $value->total(),
                'current_page' => $value->currentPage(),
                'has_more' => $value->hasMorePages(),
            ];
        }

        if ($value instanceof Paginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'current_page' => $value->currentPage(),
                'has_more' => $value->hasMorePages(),
            ];
        }

        if ($value instanceof CursorPaginator) {
            return [
                'data' => collect($value->items())->map->toArray()->values()->all(),
                'has_more' => $value->hasMorePages(),
                'cursor' => $value->cursor()?->toArray(),
            ];
        }

        if ($value instanceof EloquentCollection) {
            return $value->map->toArray()->values()->all();
        }

        if ($value instanceof Collection) {
            return $value->all(); // preserve keys (e.g. keyed pluck)
        }

        if ($value instanceof Model) {
            return $value->toArray();
        }

        return $value;
    }
}
