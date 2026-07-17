<?php

namespace NormCache\Support;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Facades\DB;
use NormCache\CacheableBuilder;

final class QueryHasher
{
    public static function forNormalizedQuery(CacheableBuilder $builder, QueryBuilder $query): string
    {
        $selectBindings = $query->getRawBindings()['select'];

        if (!empty($query->columns) || !empty($selectBindings)) {
            $query = $query->cloneWithout(['columns'])->cloneWithoutBindings(['select']);
        }

        return self::fromBuilder($builder, $query);
    }

    public static function forResultQuery(EloquentBuilder $builder, QueryBuilder $query): string
    {
        return self::fromBuilder($builder, $query);
    }

    public static function forPaginationCountQuery(CacheableBuilder $builder, QueryBuilder $query): string
    {
        if (!empty($query->orders) || !empty($query->unionOrders)) {
            $query = $query->cloneWithout(['orders', 'unionOrders'])
                ->cloneWithoutBindings(['order', 'unionOrder']);
        }

        return self::hash(self::forNormalizedQuery($builder, $query) . ':pagination_count');
    }

    public static function forScalarQuery(CacheableBuilder $builder, QueryBuilder $query, string $kind, array $columns): string
    {
        if (self::scalarKindIgnoresOrder($kind) && (!empty($query->orders) || !empty($query->unionOrders))) {
            $query = $query->cloneWithout(['orders', 'unionOrders'])
                ->cloneWithoutBindings(['order', 'unionOrder']);
        }

        $stripped = $query->cloneWithout(['columns'])->cloneWithoutBindings(['select']);

        return self::hashWith($stripped, [
            'kind' => $kind,
            'columns' => $columns,
            'casts' => $builder->getModel()->getCasts(),
        ]);
    }

    public static function forRelationQuery(
        string $stripKey,
        QueryBuilder $base,
    ): string {
        $shape = [];

        $wheres = [];
        foreach ($base->wheres as $where) {
            if (($where['column'] ?? null) !== $stripKey) {
                $wheres[] = self::normalizeValueForHash($where, $base);
            }
        }

        if ($wheres !== []) {
            $shape['wheres'] = $wheres;
        }

        if (!empty($base->joins)) {
            $shape['joins'] = array_map(fn($join) => [
                'type' => $join->type ?? null,
                'table' => self::normalizeValueForHash($join->table, $base),
                'sql' => $join->toSql(),
                'bindings' => self::normalizeValueForHash($join->getBindings(), $base),
            ], $base->joins);
        }

        foreach (['orders', 'limit', 'offset', 'groups', 'havings', 'distinct', 'unions', 'lock'] as $prop) {
            if (isset($base->{$prop}) && $base->{$prop} !== null && $base->{$prop} !== [] && $base->{$prop} !== false) {
                $shape[$prop] = self::normalizeValueForHash($base->{$prop}, $base);
            }
        }

        $nonWhereBindings = array_diff_key($base->getRawBindings(), ['where' => null]);
        if (!empty(array_filter($nonWhereBindings))) {
            $shape['bindings'] = self::normalizeValueForHash($nonWhereBindings, $base);
        }

        return self::hashPayload($shape);
    }

    public static function fromBuilder(EloquentBuilder $builder, QueryBuilder $query): string
    {
        return self::hashWith($query, [
            'casts' => $builder->getModel()->getCasts(),
        ]);
    }

    public static function fromQuery(QueryBuilder $query): string
    {
        return self::hashWith($query);
    }

    public static function hash(string $data): string
    {
        return hash('xxh128', $data);
    }

    public static function normalizeValueForHash(mixed $value, ?QueryBuilder $base = null): mixed
    {
        if ($value instanceof QueryBuilder) {
            return [
                'sql' => $value->toSql(),
                'bindings' => self::normalizeValueForHash($value->getBindings(), $value),
            ];
        }

        if ($value instanceof Expression) {
            $grammar = $base?->getGrammar() ?: DB::getQueryGrammar();

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
        return self::hash($query->toSql() . self::encodePayload(array_merge([
            'bindings' => self::normalizeValueForHash($query->getBindings()),
            'useWritePdo' => $query->useWritePdo,
        ], $extra)));
    }

    private static function hashPayload(array $payload): string
    {
        return self::hash(self::encodePayload($payload));
    }

    private static function encodePayload(array $payload): string
    {
        try {
            return json_encode($payload, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            return json_encode(self::normalizeBinaryStringsForHash($payload), JSON_THROW_ON_ERROR);
        }
    }

    private static function normalizeBinaryStringsForHash(mixed $value): mixed
    {
        if (is_string($value)) {
            return mb_check_encoding($value, 'UTF-8') ? $value : ['binary' => base64_encode($value)];
        }

        if (is_array($value)) {
            $normalized = [];
            foreach ($value as $key => $item) {
                $normalized[$key] = self::normalizeBinaryStringsForHash($item);
            }

            return $normalized;
        }

        return $value;
    }

    private static function scalarKindIgnoresOrder(string $kind): bool
    {
        return str_starts_with($kind, 'count')
            || in_array($kind, ['sum', 'avg', 'min', 'max', 'exists'], true);
    }
}
