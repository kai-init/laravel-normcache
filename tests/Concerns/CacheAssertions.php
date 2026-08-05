<?php

namespace NormCache\Tests\Concerns;

use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Collection as EloquentCollection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Pagination\CursorPaginator;
use Illuminate\Pagination\Paginator;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
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

    /**
     * @return list<array<string, mixed>>
     */
    protected function contract(callable $cached, callable $native, bool $expectNoStrayQueries = false): array
    {
        $expected = $this->normalize($native());
        $cold = $this->normalize($cached());

        DB::flushQueryLog();
        DB::enableQueryLog();

        try {
            $warm = $this->normalize($cached());
            $strayQueries = DB::getQueryLog();
        } finally {
            DB::disableQueryLog();
        }

        $this->assertSame($expected, $cold, 'cold cache result differs from native Eloquent');
        $this->assertSame($cold, $warm, 'warm cache result differs from cold');

        if ($expectNoStrayQueries) {
            $this->assertSame([], $strayQueries, 'expected no SQL queries on the warm cache path');
        }

        return $strayQueries;
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
