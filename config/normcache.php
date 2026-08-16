<?php

return [
    'enabled' => env('NORMCACHE_ENABLED', true),
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),
    'key_prefix' => env('NORMCACHE_KEY_PREFIX', ''),
    'serializer' => env('NORMCACHE_SERIALIZER', 'auto'),

    'row_ttl' => (int) env('NORMCACHE_ROW_TTL', 604800),
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),

    // Zero disables automatic result overlays.
    'auto_overlay_max_rows' => 1000,

    'revalidation' => (bool) env('NORMCACHE_REVALIDATION', true),

    'building_lock_ttl' => 5,
    'stampede_wait_ms' => 200,

    'events' => (bool) env('NORMCACHE_EVENTS', false),
    'debugbar' => (bool) env('NORMCACHE_DEBUGBAR', false),
];
