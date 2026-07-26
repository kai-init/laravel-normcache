<?php

return [
    'enabled' => env('NORMCACHE_ENABLED', true),
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),
    'key_prefix' => env('NORMCACHE_PREFIX', ''),

    'ttl' => (int) env('NORMCACHE_TTL', 604800),
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),

    'primary_keys' => [],
    'deployment_ids' => [],

    'max_membership_rows' => 1000,
    'max_membership_bytes' => 1048576,
    'max_canonical_bytes' => 16777216,
    'max_result_bytes' => 4194304,
    'max_precise_invalidation_pks' => 1000,

    'building_lock_ttl' => (int) env('NORMCACHE_BUILDING_LOCK_TTL', 5),
    'stampede_wait_ms' => (int) env('NORMCACHE_STAMPEDE_WAIT_MS', 200),
    'stampede_wake_tokens' => (int) env('NORMCACHE_STAMPEDE_WAKE_TOKENS', 64),
    'publication_guard_margin_seconds' => 10,

    'events' => (bool) env('NORMCACHE_EVENTS', false),
    'debugbar' => (bool) env('NORMCACHE_DEBUGBAR', false),
];
