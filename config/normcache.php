<?php

return [
    'enabled' => env('NORMCACHE_ENABLED', true),
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),
    'key_prefix' => env('NORMCACHE_KEY_PREFIX', ''),
    'serializer' => env('NORMCACHE_SERIALIZER', 'auto'),

    'row_ttl' => (int) env('NORMCACHE_ROW_TTL', 604800),
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),

    // Set to 0 to disable automatic result overlays.
    'auto_overlay_max_rows' => 1000,

    'max_precise_invalidation_keys' => 1000,

    'building_lock_ttl' => 5,
    'stampede_wait_ms' => 200,
    'stampede_wake_tokens' => 64,
    'epoch_refresh_seconds' => 5,

    'events' => (bool) env('NORMCACHE_EVENTS', false),
    'debugbar' => (bool) env('NORMCACHE_DEBUGBAR', false),
];
