<?php

namespace NormCache\Values;

final readonly class CacheConfig
{
    public function __construct(
        public string $connection,
        public string $keyPrefix,
        public string $serializer,
        public int $rowTtl,
        public int $queryTtl,
        public int $maxAutoOverlayRows,
        public int $buildingLockTtl,
        public int $stampedeWaitMs,
        public bool $enabled,
        public bool $dispatchEvents,
        public bool $debugbar,
        public bool $revalidation,
    ) {}

    public function wakeTtl(): int
    {
        return $this->buildingLockTtl + (int) ceil($this->stampedeWaitMs / 1000) + 5;
    }

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
            1000,
        );
        $buildingLockTtl = self::positive($values, 'building_lock_ttl', 5);
        $stampedeWaitMs = self::positive($values, 'stampede_wait_ms', 200);

        return new self(
            connection: (string) ($values['connection'] ?? 'cache'),
            keyPrefix: $keyPrefix,
            serializer: self::serializer($values['serializer'] ?? 'auto'),
            rowTtl: $rowTtl,
            queryTtl: $queryTtl,
            maxAutoOverlayRows: $maxAutoOverlayRows,
            buildingLockTtl: $buildingLockTtl,
            stampedeWaitMs: $stampedeWaitMs,
            enabled: (bool) ($values['enabled'] ?? true),
            dispatchEvents: (bool) ($values['events'] ?? false),
            debugbar: (bool) ($values['debugbar'] ?? false),
            revalidation: (bool) ($values['revalidation'] ?? true),
        );
    }

    private static function serializer(mixed $value): string
    {
        if (!is_string($value) || !in_array($value, ['auto', 'php', 'igbinary'], true)) {
            throw new \InvalidArgumentException(
                'NormCache serializer must be auto, php, or igbinary.',
            );
        }

        return $value;
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
}
