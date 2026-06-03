<?php

namespace NormCache\Support;

final class AttributeProjector
{
    /** @param list<string> $columns */
    public static function normalizeProjection(array $columns): array
    {
        $normalized = [];

        foreach ($columns as $column) {
            [$source, $output] = self::parseProjection((string) $column);
            $normalized[$output] = $source;
        }

        return $normalized;
    }

    /**
     * @param  array<string, mixed>  $attributes
     * @param  array<string, string>  $projection
     */
    public static function projectAttributes(array $attributes, array $projection): array
    {
        $projected = [];

        foreach ($projection as $output => $source) {
            if (array_key_exists($source, $attributes)) {
                $projected[$output] = $attributes[$source];
            }
        }

        return $projected;
    }

    private static function parseProjection(string $column): array
    {
        $column = trim($column);
        $segments = preg_split('/\s+as\s+/i', $column);

        if (count($segments) === 2) {
            return [self::unqualify($segments[0]), self::unqualify($segments[1])];
        }

        $name = self::unqualify($column);

        return [$name, $name];
    }

    private static function unqualify(string $column): string
    {
        $column = trim($column);
        $dotPos = strrpos($column, '.');

        if ($dotPos !== false) {
            $column = substr($column, $dotPos + 1);
        }

        return trim($column, " \t\n\r\0\x0B`\"[]");
    }
}
