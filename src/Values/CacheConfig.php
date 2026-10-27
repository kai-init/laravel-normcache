<?php

namespace NormCache\Values;

final readonly class CacheConfig
{
    public const MAX_PRECISE_INVALIDATION_KEYS = 1000;

    public const MAX_STAMPEDE_WAKE_TOKENS = 1000;

    public function __construct(
        public string $connection,
        public string $keyPrefix,
        public string $serializer,
        public int $rowTtl,
        public int $queryTtl,
        public int $maxAutoOverlayRows,
        public int $maxPreciseInvalidationKeys,
        public int $buildingLockTtl,
        public int $stampedeWaitMs,
        public int $stampedeWakeTokens,
        public int $epochRefreshSeconds,
        public bool $enabled,
        public bool $dispatchEvents,
        public bool $debugbar,
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

        return new self(
            connection: (string) ($values['connection'] ?? 'cache'),
            keyPrefix: $keyPrefix,
            serializer: self::serializer($values['serializer'] ?? 'auto'),
            rowTtl: self::integer($values, 'row_ttl', 604_800),
            queryTtl: self::integer($values, 'query_ttl', 3_600),
            maxAutoOverlayRows: self::integer($values, 'auto_overlay_max_rows', 1000, 0),
            maxPreciseInvalidationKeys: self::integer(
                $values,
                'max_precise_invalidation_keys',
                self::MAX_PRECISE_INVALIDATION_KEYS,
                maximum: self::MAX_PRECISE_INVALIDATION_KEYS,
            ),
            buildingLockTtl: self::integer($values, 'building_lock_ttl', 5),
            stampedeWaitMs: self::integer($values, 'stampede_wait_ms', 200),
            stampedeWakeTokens: self::integer(
                $values,
                'stampede_wake_tokens',
                64,
                maximum: self::MAX_STAMPEDE_WAKE_TOKENS,
            ),
            epochRefreshSeconds: self::integer($values, 'epoch_refresh_seconds', 5, 0),
            enabled: (bool) ($values['enabled'] ?? true),
            dispatchEvents: (bool) ($values['events'] ?? false),
            debugbar: (bool) ($values['debugbar'] ?? false),
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
    private static function integer(
        array $values,
        string $key,
        int $default,
        int $minimum = 1,
        ?int $maximum = null,
    ): int {
        $value = (int) ($values[$key] ?? $default);

        if ($value < $minimum) {
            throw new \InvalidArgumentException("NormCache {$key} must be at least {$minimum}.");
        }

        if ($maximum !== null && $value > $maximum) {
            throw new \InvalidArgumentException("NormCache {$key} must not exceed {$maximum}.");
        }

        return $value;
    }
}
