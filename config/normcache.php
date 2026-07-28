<?php

return [
    'enabled' => env('NORMCACHE_ENABLED', true),
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),
    'key_prefix' => env('NORMCACHE_KEY_PREFIX', ''),

    'row_ttl' => (int) env('NORMCACHE_ROW_TTL', 604800),
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),

    // Each group requires connection, database, and table metadata. Add schema to
    // restrict a PostgreSQL or SQL Server match. Types are integer or string.
    'primary_keys' => [
        // [
        //     'connection' => 'pgsql',
        //     'database' => 'app',
        //     'schema' => 'public',
        //     'tables' => [
        //         'events' => ['column' => 'event_id', 'type' => 'string'],
        //         'orders' => ['column' => 'order_id', 'type' => 'integer'],
        //     ],
        // ],
    ],

    'max_precise_invalidation_keys' => 1000,

    'building_lock_ttl' => (int) env('NORMCACHE_BUILDING_LOCK_TTL', 5),
    'stampede_wait_ms' => (int) env('NORMCACHE_STAMPEDE_WAIT_MS', 200),
    'stampede_wake_tokens' => (int) env('NORMCACHE_STAMPEDE_WAKE_TOKENS', 64),

    'events' => (bool) env('NORMCACHE_EVENTS', false),
    'debugbar' => (bool) env('NORMCACHE_DEBUGBAR', false),
];
