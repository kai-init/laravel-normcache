<?php

namespace NormCache\Values;

/**
 * Runtime configuration for the cache manager. Mutable so tests and runtime
 * toggles (fallback, cooldown experiments) can adjust knobs on the live instance.
 */
final class CacheConfig
{
    public function __construct(
        public int $ttl,
        public int $queryTtl,
        public int $cooldown = 0,
        public bool $enabled = true,
        public bool $fallbackEnabled = false,
        public bool $dispatchEvents = true,
        public bool $cluster = false,
        public bool $slotting = false,
    ) {}
}
