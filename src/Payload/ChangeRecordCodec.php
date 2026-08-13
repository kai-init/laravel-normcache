<?php

namespace NormCache\Payload;

use NormCache\Values\ChangeRecord;

final class ChangeRecordCodec
{
    private const FORMAT = 1;

    /** @param list<string> $columns */
    public function encode(string $mutation, array $columns, bool $precise): string
    {
        sort($columns, SORT_STRING);

        return json_encode([
            'f' => self::FORMAT,
            'm' => $mutation,
            'c' => $columns,
            'p' => $precise,
        ], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    public function decode(string $payload): ChangeRecord
    {
        try {
            $envelope = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
        } catch (\JsonException) {
            return ChangeRecord::corrupt();
        }

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== self::FORMAT
            || !is_string($envelope['m'] ?? null)
            || !is_array($envelope['c'] ?? null)
            || !is_bool($envelope['p'] ?? null)
        ) {
            return ChangeRecord::corrupt();
        }

        $columns = [];

        foreach ($envelope['c'] as $column) {
            if (!is_string($column)) {
                return ChangeRecord::corrupt();
            }

            $columns[] = $column;
        }

        return new ChangeRecord(
            valid: true,
            mutation: $envelope['m'],
            columns: $columns,
            precise: $envelope['p'],
        );
    }
}
