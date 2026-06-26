<?php

return [
    // Redis connection from config/database.php.
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),

    // Master switch; false bypasses the cache.
    'enabled' => env('NORMCACHE_ENABLED', true),

    // Model attribute payload TTL (seconds).
    'ttl' => (int) env('NORMCACHE_TTL', 604800),

    // Query, result, pivot, and through-cache TTL (seconds).
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),

    // Prefix every NormCache key. Useful when sharing a Redis database.
    'key_prefix' => env('NORMCACHE_PREFIX', ''),

    // Debounce automatic version bumps on write-heavy models; 0 bumps immediately (seconds).
    'cooldown' => (int) env('NORMCACHE_COOLDOWN', 0),

    // Build-lock expiry (seconds).
    'building_lock_ttl' => (int) env('NORMCACHE_BUILDING_LOCK_TTL', 5),

    // Max wait for another request's build wake signal (milliseconds).
    'stampede_wait_ms' => (int) env('NORMCACHE_STAMPEDE_WAIT_MS', 200),

    // Wake tokens pushed when a cache build releases. Raise for high same-key concurrency.
    'stampede_wake_tokens' => (int) env('NORMCACHE_STAMPEDE_WAKE_TOKENS', 64),

    // Dispatch cache hit, miss, and bypass events. Enable only if something consumes them.
    'events' => (bool) env('NORMCACHE_EVENTS', false),

    // true fails open to DB on Redis errors; false re-throws them.
    'fallback' => (bool) env('NORMCACHE_FALLBACK', true),

    // Fire retrieved for cached models if observers depend on that Eloquent event.
    'fire_retrieved' => (bool) env('NORMCACHE_FIRE_RETRIEVED', false),

    // Register the Laravel Debugbar collector for local cache inspection.
    'debugbar' => env('NORMCACHE_DEBUGBAR', false),
];
