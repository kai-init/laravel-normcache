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

    public static function forRelationQuery(EloquentBuilder $builder, string $stripKey): string
    {
        $base = $builder->toBase();
        $shape = [];

        $rawBindings = $base->getRawBindings();
        $whereBindings = $rawBindings['where'] ?? [];
        $newWhereBindings = [];
        $bindingOffset = 0;

        // Surgical strip of the parent foreign key constraint
        $wheres = [];
        foreach ($base->wheres as $where) {
            $count = self::countBindingsForWhere($where);
            $column = $where['column'] ?? null;

            if ($column !== $stripKey) {
                $wheres[] = self::normalizeWhereForHash($where, $base);
                for ($i = 0; $i < $count; $i++) {
                    if (isset($whereBindings[$bindingOffset + $i])) {
                        $newWhereBindings[] = $whereBindings[$bindingOffset + $i];
                    }
                }
            }
            $bindingOffset += $count;
        }

        if ($wheres !== []) {
            $shape['wheres'] = $wheres;
        }

        if (!empty($base->joins)) {
            $shape['joins'] = array_map(fn ($join) => [
                'type' => $join->type ?? null,
                'table' => is_string($join->table) ? $join->table : (string) $join->table,
                'sql' => $join->toSql(),
                'bindings' => self::normalizeValueForHash($join->getBindings(), $base),
            ], $base->joins);
        }

        foreach (['orders', 'limit', 'offset', 'groups', 'havings', 'distinct', 'unions', 'lock'] as $prop) {
            if (isset($base->{$prop}) && $base->{$prop} !== null && $base->{$prop} !== [] && $base->{$prop} !== false) {
                $shape[$prop] = self::normalizeValueForHash($base->{$prop}, $base);
            }
        }

        $rawBindings['where'] = $newWhereBindings;
        $shape['bindings'] = self::normalizeValueForHash($rawBindings, $base);

        return self::hash(json_encode($shape, JSON_THROW_ON_ERROR));
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

    public static function normalizeValueForHash(mixed $value, ?QueryBuilder $base = null): mixed
    {
        if ($value instanceof QueryBuilder) {
            return [
                'sql' => $value->toSql(),
                'bindings' => self::normalizeValueForHash($value->getBindings(), $value),
            ];
        }

        if ($value instanceof \Illuminate\Contracts\Database\Query\Expression) {
            $grammar = $base?->getGrammar() ?: \Illuminate\Support\Facades\DB::getQueryGrammar();

            return [
                'expression' => (string) $value->getValue($grammar),
            ];
        }

        if ($value instanceof \BackedEnum) {
            return $value->value;
        }

        if ($value instanceof \Stringable) {
            return (string) $value;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d H:i:s');
        }

        if (is_array($value)) {
            $normalized = [];
            foreach ($value as $key => $item) {
                $normalized[$key] = self::normalizeValueForHash($item, $base);
            }

            return $normalized;
        }

        if (is_object($value)) {
            return [
                'class' => $value::class,
                'value' => method_exists($value, '__toString') ? (string) $value : null,
            ];
        }

        return $value;
    }

    private static function hashWith(QueryBuilder $query, array $extra = []): string
    {
        return self::hash($query->toSql() . json_encode(array_merge([
            'bindings' => self::normalizeValueForHash($query->getBindings()),
            'useWritePdo' => $query->useWritePdo,
        ], $extra), JSON_THROW_ON_ERROR));
    }

    private static function scalarKindIgnoresOrder(string $kind): bool
    {
        return str_starts_with($kind, 'count')
            || in_array($kind, ['sum', 'avg', 'min', 'max', 'exists'], true);
    }

    private static function normalizeWhereForHash(array $where, QueryBuilder $base): array
    {
        $type = strtolower($where['type'] ?? '');

        if ($type === 'nested' && isset($where['query'])) {
            return [
                'type' => 'nested',
                'boolean' => $where['boolean'] ?? 'and',
                'wheres' => array_map(fn ($w) => self::normalizeWhereForHash($w, $base), $where['query']->wheres ?? []),
            ];
        }

        if (in_array($type, ['exists', 'notexists', 'sub'], true) && isset($where['query'])) {
            return [
                'type' => $type,
                'boolean' => $where['boolean'] ?? 'and',
                'sql' => $where['query']->toSql(),
                'bindings' => self::normalizeValueForHash($where['query']->getBindings(), $base),
            ];
        }

        if ($type === 'raw') {
            return [
                'type' => 'raw',
                'sql' => $where['sql'] ?? null,
                'boolean' => $where['boolean'] ?? 'and',
            ];
        }

        return [
            'type' => $type,
            'column' => is_string($where['column'] ?? null) ? $where['column'] : null,
            'operator' => $where['operator'] ?? null,
            'boolean' => $where['boolean'] ?? 'and',
        ];
    }

    private static function countBindingsForWhere(array $where): int
    {
        $type = strtolower($where['type'] ?? '');

        if (in_array($type, ['sub', 'exists', 'notexists'], true)) {
            return count($where['query']->getBindings());
        }

        if (in_array($type, ['basic', 'date', 'month', 'day', 'year', 'time'], true)) {
            return 1;
        }

        if (in_array($type, ['in', 'notin'], true)) {
            return count($where['values'] ?? []);
        }

        if ($type === 'between') {
            return 2;
        }

        if ($type === 'nested') {
            return array_reduce($where['query']->wheres ?? [], fn ($c, $w) => $c + self::countBindingsForWhere($w), 0);
        }

        return 0;
    }
}
