<?php

namespace NormCache\Values;

final readonly class CacheConfig
{
    public const MAX_PRECISE_INVALIDATION_KEYS = 1000;

    public function __construct(
        public string $connection,
        public string $keyPrefix,
        public int $rowTtl,
        public int $queryTtl,
        public int $maxAutoOverlayRows,
        public array $primaryKeys,
        public int $maxPreciseInvalidationKeys,
        public int $buildingLockTtl,
        public int $stampedeWaitMs,
        public int $stampedeWakeTokens,
        public bool $enabled,
        public bool $dispatchEvents,
        public bool $debugbar,
    ) {}

    /** @param array<string, mixed> $values */
    public static function fromArray(array $values): self
    {
        $keyPrefix = (string) ($values['key_prefix'] ?? '');

        if (str_contains($keyPrefix, '{') || str_contains($keyPrefix, '}')) {
            throw new \InvalidArgumentException('NormCache key_prefix must not contain Redis hash-tag braces.');
        }

        $rowTtl = self::positive($values, 'row_ttl', 604_800);
        $queryTtl = self::positive($values, 'query_ttl', 3_600);
        $maxAutoOverlayRows = self::nonNegative(
            $values,
            'auto_overlay_max_rows',
            50,
        );
        $maxPreciseInvalidationKeys = self::bounded(
            $values,
            'max_precise_invalidation_keys',
            self::MAX_PRECISE_INVALIDATION_KEYS,
        );
        $buildingLockTtl = self::positive($values, 'building_lock_ttl', 5);
        $stampedeWaitMs = self::positive($values, 'stampede_wait_ms', 200);
        $stampedeWakeTokens = self::positive($values, 'stampede_wake_tokens', 64);

        return new self(
            connection: (string) ($values['connection'] ?? 'cache'),
            keyPrefix: $keyPrefix,
            rowTtl: $rowTtl,
            queryTtl: $queryTtl,
            maxAutoOverlayRows: $maxAutoOverlayRows,
            primaryKeys: self::primaryKeys($values['primary_keys'] ?? []),
            maxPreciseInvalidationKeys: $maxPreciseInvalidationKeys,
            buildingLockTtl: $buildingLockTtl,
            stampedeWaitMs: $stampedeWaitMs,
            stampedeWakeTokens: $stampedeWakeTokens,
            enabled: (bool) ($values['enabled'] ?? true),
            dispatchEvents: (bool) ($values['events'] ?? false),
            debugbar: (bool) ($values['debugbar'] ?? false),
        );
    }

    /** @param array<string, mixed> $values */
    private static function positive(array $values, string $key, int $default): int
    {
        $value = (int) ($values[$key] ?? $default);

        if ($value < 1) {
            throw new \InvalidArgumentException("NormCache {$key} must be at least 1.");
        }

        return $value;
    }

    /** @param array<string, mixed> $values */
    private static function nonNegative(array $values, string $key, int $default): int
    {
        $value = (int) ($values[$key] ?? $default);

        if ($value < 0) {
            throw new \InvalidArgumentException("NormCache {$key} must be at least 0.");
        }

        return $value;
    }

    /** @param array<string, mixed> $values */
    private static function bounded(array $values, string $key, int $maximum): int
    {
        $value = self::positive($values, $key, $maximum);

        if ($value > $maximum) {
            throw new \InvalidArgumentException("NormCache {$key} must not exceed {$maximum}.");
        }

        return $value;
    }

    /** @return list<array<string, string>> */
    private static function primaryKeys(mixed $value): array
    {
        if (!is_array($value)) {
            throw new \InvalidArgumentException('NormCache primary_keys must be an array.');
        }

        $result = [];

        foreach ($value as $group) {
            if (!is_array($group)) {
                throw new \InvalidArgumentException(
                    'Each NormCache primary_keys entry must be a structured array.',
                );
            }

            array_push($result, ...self::primaryKeyGroup($group));
        }

        return $result;
    }

    /** @param array<string, mixed> $group
     * @return list<array<string, string>>
     */
    private static function primaryKeyGroup(array $group): array
    {
        foreach (['connection', 'database'] as $field) {
            if (!is_string($group[$field] ?? null) || $group[$field] === '') {
                throw new \InvalidArgumentException(
                    "NormCache primary_keys groups require a non-empty {$field}.",
                );
            }
        }

        if (array_key_exists('schema', $group) && !is_string($group['schema'])) {
            throw new \InvalidArgumentException(
                'NormCache primary_keys group schema must be a string when present.',
            );
        }

        $tables = $group['tables'] ?? null;

        if (!is_array($tables) || $tables === []) {
            throw new \InvalidArgumentException(
                'NormCache primary_keys groups require a non-empty tables array.',
            );
        }

        $result = [];

        foreach ($tables as $table => $metadata) {
            if (!is_string($table) || $table === '') {
                throw new \InvalidArgumentException(
                    'NormCache primary_keys table names must be non-empty strings.',
                );
            }

            if (!is_array($metadata)) {
                throw new \InvalidArgumentException(
                    'NormCache primary_keys table metadata must be a structured array.',
                );
            }

            $override = [
                'connection' => $group['connection'],
                'database' => $group['database'],
                'table' => $table,
                'column' => $metadata['column'] ?? null,
                'type' => $metadata['type'] ?? null,
            ];

            if (array_key_exists('schema', $group)) {
                $override['schema'] = $group['schema'];
            }

            $result[] = self::primaryKey($override);
        }

        return $result;
    }

    /** @param array<string, mixed> $override
     * @return array<string, string>
     */
    private static function primaryKey(array $override): array
    {
        foreach (['connection', 'database', 'table', 'column', 'type'] as $field) {
            if (!is_string($override[$field] ?? null) || $override[$field] === '') {
                throw new \InvalidArgumentException(
                    "NormCache primary_keys entries require a non-empty {$field}.",
                );
            }
        }

        if (
            array_key_exists('schema', $override)
            && !is_string($override['schema'])
        ) {
            throw new \InvalidArgumentException(
                'NormCache primary_keys schema must be a string when present.',
            );
        }

        if (!in_array($override['type'], ['integer', 'string'], true)) {
            throw new \InvalidArgumentException(
                'NormCache primary_keys type must be integer or string.',
            );
        }

        return $override;
    }
}
