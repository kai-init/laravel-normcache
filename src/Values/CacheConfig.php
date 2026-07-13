<?php

namespace NormCache\Values;

/**
 * Runtime configuration for the cache manager. Mutable so tests, fallback,
 * and cooldown experiments can adjust the live request-scoped instance.
 */
final class CacheConfig
{
    public function __construct(
        public int $ttl,
        public int $queryTtl,
        public int $cooldown = 0,
        public bool $enabled = true,
        public bool $fallbackEnabled = true,
        public bool $dispatchEvents = true,
        public int $stampedeWakeTokens = 64,
    ) {}

    // Live runtime toggle; only payload reads honor it — pivot/standalone version reads always check scheduled keys.
    public function cooldownEnabled(): bool
    {
        return $this->cooldown > 0;
    }
}
