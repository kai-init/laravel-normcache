<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;

final class QueryHasher
{
    public static function forNormalizedQuery(CacheableBuilder $builder, ?QueryBuilder $query = null): string
    {
        return self::fromBuilder($builder, ($query ?? $builder->toBase())->cloneWithout(['columns']));
    }

    public static function forResultQuery(EloquentBuilder $builder, ?QueryBuilder $query = null): string
    {
        return self::fromBuilder($builder, $query);
    }

    public static function forPaginationCountQuery(CacheableBuilder $builder, ?QueryBuilder $query = null): string
    {
        return self::hash(self::forNormalizedQuery($builder, $query) . ':pagination_count');
    }

    public static function forScalarQuery(CacheableBuilder $builder, ?QueryBuilder $query, string $kind, array $columns): string
    {
        $query ??= $builder->toBase();

        if (self::scalarKindIgnoresOrder($kind) && (!empty($query->orders) || !empty($query->unionOrders))) {
            $query = $query->cloneWithout(['orders', 'unionOrders'])
                ->cloneWithoutBindings(['order', 'unionOrder']);
        }

        return self::hash(
            self::forNormalizedQuery($builder, $query) . ':' . $kind . ':' . json_encode($columns, JSON_THROW_ON_ERROR)
        );
    }

    public static function fromBuilder(EloquentBuilder $builder, ?QueryBuilder $query = null): string
    {
        return self::hashWith($query ?? $builder->toBase(), [
            'casts' => $builder->getModel()->getCasts(),
        ]);
    }

    /** Low-level query hash (no model state). Primarily for tests. */
    public static function fromQuery(QueryBuilder $query): string
    {
        return self::hashWith($query);
    }

    public static function hash(string $data): string
    {
        return hash('xxh3', $data);
    }

    private static function hashWith(QueryBuilder $query, array $extra = []): string
    {
        return self::hash($query->toSql() . json_encode(array_merge([
            'bindings' => array_map([self::class, 'normalizeBinding'], $query->getBindings()),
            'useWritePdo' => $query->useWritePdo,
        ], $extra), JSON_THROW_ON_ERROR));
    }

    private static function scalarKindIgnoresOrder(string $kind): bool
    {
        return str_starts_with($kind, 'count')
            || in_array($kind, ['sum', 'avg', 'min', 'max', 'exists'], true);
    }

    private static function normalizeBinding(mixed $binding): mixed
    {
        if ($binding instanceof \DateTimeInterface) {
            return $binding->format('Y-m-d H:i:s.uP');
        }
        if ($binding instanceof \BackedEnum) {
            return $binding->value;
        }
        if ($binding instanceof \Stringable) {
            return (string) $binding;
        }
        if (is_string($binding) && !mb_detect_encoding($binding, 'UTF-8', true)) {
            return base64_encode($binding);
        }

        return $binding;
    }
}
