<?php

namespace NormCache\Values;

use InvalidArgumentException;

final readonly class CacheConfig
{
    public const MAX_MEMBERSHIP_ROWS = 1000;

    public const MAX_MEMBERSHIP_BYTES = 1_048_576;

    public const MAX_CANONICAL_BYTES = 16_777_216;

    public const MAX_RESULT_BYTES = 4_194_304;

    public const MAX_PRECISE_INVALIDATION_PKS = 1000;

    public function __construct(
        public string $connection,
        public string $keyPrefix,
        public int $ttl,
        public int $queryTtl,
        public array $primaryKeys,
        public array $deploymentIds,
        public int $maxMembershipRows,
        public int $maxMembershipBytes,
        public int $maxCanonicalBytes,
        public int $maxResultBytes,
        public int $maxPreciseInvalidationPks,
        public int $buildingLockTtl,
        public int $stampedeWaitMs,
        public int $stampedeWakeTokens,
        public int $publicationGuardMarginSeconds,
        public bool $enabled,
        public bool $dispatchEvents,
        public bool $debugbar,
    ) {}

    /** @param array<string, mixed> $values */
    public static function fromArray(array $values): self
    {
        $keyPrefix = (string) ($values['key_prefix'] ?? '');

        if (str_contains($keyPrefix, '{') || str_contains($keyPrefix, '}')) {
            throw new InvalidArgumentException('NormCache key_prefix must not contain Redis hash-tag braces.');
        }

        $ttl = self::positive($values, 'ttl', 604_800);
        $queryTtl = self::positive($values, 'query_ttl', 3_600);
        $maxMembershipRows = self::bounded(
            $values,
            'max_membership_rows',
            self::MAX_MEMBERSHIP_ROWS,
        );
        $maxMembershipBytes = self::bounded(
            $values,
            'max_membership_bytes',
            self::MAX_MEMBERSHIP_BYTES,
        );
        $maxCanonicalBytes = self::bounded(
            $values,
            'max_canonical_bytes',
            self::MAX_CANONICAL_BYTES,
        );
        $maxResultBytes = self::bounded(
            $values,
            'max_result_bytes',
            self::MAX_RESULT_BYTES,
        );
        $maxPreciseInvalidationPks = self::bounded(
            $values,
            'max_precise_invalidation_pks',
            self::MAX_PRECISE_INVALIDATION_PKS,
        );
        $buildingLockTtl = self::positive($values, 'building_lock_ttl', 5);
        $stampedeWaitMs = self::positive($values, 'stampede_wait_ms', 200);
        $stampedeWakeTokens = self::positive($values, 'stampede_wake_tokens', 64);
        $publicationGuardMarginSeconds = (int) ($values['publication_guard_margin_seconds'] ?? 10);

        if ($publicationGuardMarginSeconds < 10) {
            throw new InvalidArgumentException(
                'NormCache publication_guard_margin_seconds must be at least 10.'
            );
        }

        return new self(
            connection: (string) ($values['connection'] ?? 'cache'),
            keyPrefix: $keyPrefix,
            ttl: $ttl,
            queryTtl: $queryTtl,
            primaryKeys: self::primaryKeys($values['primary_keys'] ?? []),
            deploymentIds: self::deploymentIds($values['deployment_ids'] ?? []),
            maxMembershipRows: $maxMembershipRows,
            maxMembershipBytes: $maxMembershipBytes,
            maxCanonicalBytes: $maxCanonicalBytes,
            maxResultBytes: $maxResultBytes,
            maxPreciseInvalidationPks: $maxPreciseInvalidationPks,
            buildingLockTtl: $buildingLockTtl,
            stampedeWaitMs: $stampedeWaitMs,
            stampedeWakeTokens: $stampedeWakeTokens,
            publicationGuardMarginSeconds: $publicationGuardMarginSeconds,
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
            throw new InvalidArgumentException("NormCache {$key} must be at least 1.");
        }

        return $value;
    }

    /** @param array<string, mixed> $values */
    private static function bounded(array $values, string $key, int $maximum): int
    {
        $value = self::positive($values, $key, $maximum);

        if ($value > $maximum) {
            throw new InvalidArgumentException("NormCache {$key} must not exceed {$maximum}.");
        }

        return $value;
    }

    /** @return list<array<string, string>> */
    private static function primaryKeys(mixed $value): array
    {
        if (!is_array($value)) {
            throw new InvalidArgumentException('NormCache primary_keys must be an array.');
        }

        $result = [];

        foreach ($value as $override) {
            if (!is_array($override)) {
                throw new InvalidArgumentException(
                    'Each NormCache primary_keys entry must be a structured array.',
                );
            }

            foreach (['connection', 'database', 'table', 'column', 'type'] as $field) {
                if (!is_string($override[$field] ?? null) || $override[$field] === '') {
                    throw new InvalidArgumentException(
                        "NormCache primary_keys entries require a non-empty {$field}.",
                    );
                }
            }

            if (
                array_key_exists('schema', $override)
                && !is_string($override['schema'])
            ) {
                throw new InvalidArgumentException(
                    'NormCache primary_keys schema must be a string when present.',
                );
            }

            if (!in_array($override['type'], ['integer', 'string'], true)) {
                throw new InvalidArgumentException(
                    'NormCache primary_keys type must be integer or string.',
                );
            }

            $result[] = $override;
        }

        return $result;
    }

    /** @return array<string, string> */
    private static function deploymentIds(mixed $value): array
    {
        if (!is_array($value)) {
            throw new InvalidArgumentException('NormCache deployment_ids must be an array.');
        }

        foreach ($value as $connection => $deployment) {
            if (
                !is_string($connection)
                || $connection === ''
                || !is_string($deployment)
                || $deployment === ''
            ) {
                throw new InvalidArgumentException(
                    'NormCache deployment_ids must map connection names to non-empty strings.',
                );
            }
        }

        return $value;
    }
}
