<?php

namespace NormCache\Support;

use Illuminate\Database\Query\Builder as QueryBuilder;

final class QueryHasher
{
    public static function fromQuery(QueryBuilder $query): string
    {
        return self::hash($query->toSql() . json_encode([
            'bindings' => array_map([self::class, 'normalizeBinding'], $query->getBindings()),
            'useWritePdo' => $query->useWritePdo,
        ], JSON_THROW_ON_ERROR));
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

    public static function hash(string $data): string
    {
        return hash('xxh3', $data);
    }
}
