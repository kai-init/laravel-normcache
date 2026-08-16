<?php

namespace NormCache\Support;

// Normalizes columns only; table identifier case is identity-bearing.
final class ColumnName
{
    /**
     * Preserves JSON paths because their predicate and assignment semantics differ.
     *
     * @return list<string>
     */
    public static function segments(string $column): array
    {
        return array_map(
            static fn(string $part): string => strtolower(trim($part, " \t\n\r\0\x0B`\"[]")),
            explode('.', trim($column)),
        );
    }

    public static function unqualified(string $column): string
    {
        $segments = self::segments($column);

        return (string) end($segments);
    }
}
