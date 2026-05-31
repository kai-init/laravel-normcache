<?php

return [
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled' => env('NORMCACHE_ENABLED', true),
    'ttl' => (int) env('NORMCACHE_TTL', 604800),
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),
    'key_prefix' => env('NORMCACHE_PREFIX', ''),
    'cooldown' => (int) env('NORMCACHE_COOLDOWN', 0),
    'building_lock_ttl' => (int) env('NORMCACHE_BUILDING_LOCK_TTL', 5),
    'stampede_wait_ms' => (int) env('NORMCACHE_STAMPEDE_WAIT_MS', 200),
    'cluster' => (bool) env('NORMCACHE_CLUSTER', false),
    'events' => (bool) env('NORMCACHE_EVENTS', true),
    'fallback' => (bool) env('NORMCACHE_FALLBACK', false),
    'fire_retrieved' => (bool) env('NORMCACHE_FIRE_RETRIEVED', false),
    'debugbar' => env('NORMCACHE_DEBUGBAR', false),
    'stale_version_depth' => (int) env('NORMCACHE_STALE_VERSION_DEPTH', (int) env('NORMCACHE_STALE_TTL_DEPTH', 3)),
];
