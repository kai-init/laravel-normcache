<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryHasher
{
    public static function forNormalizedQuery(QueryBuilder $query): string
    {
        $cols = $query->columns;
        $query->columns = null;
        try {
            return self::fromQuery($query);
        } finally {
            $query->columns = $cols;
        }
    }

    public static function forResultQuery(QueryBuilder $query): string
    {
        return self::fromQuery($query);
    }

    public static function forPaginationCountQuery(QueryBuilder $query): string
    {
        $cols = $query->columns;
        $query->columns = null;
        try {
            return self::hash(self::fromQuery($query) . ':pagination_count');
        } finally {
            $query->columns = $cols;
        }
    }

    public static function forScalarQuery(QueryBuilder $query, string $kind, array $columns): string
    {
        // Clear columns because the scalar operation will replace them anyway.
        // This ensures select('foo')->count() and select('bar')->count() hash identically.
        $cols = $query->columns;
        $query->columns = null;
        try {
            return self::hash(self::fromQuery($query) . ':' . $kind . ':' . json_encode($columns, JSON_THROW_ON_ERROR));
        } finally {
            $query->columns = $cols;
        }
    }

    public static function fromQuery(QueryBuilder $query): string
    {
        return self::hash($query->toSql() . json_encode([
            'bindings' => array_map([self::class, 'normalizeBinding'], $query->getBindings()),
            'useWritePdo' => $query->useWritePdo,
        ], JSON_THROW_ON_ERROR));
    }

    public static function hash(string $data): string
    {
        return hash('xxh3', $data);
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
