<?php

return [
    'enabled' => env('NORMCACHE_ENABLED', true),
    'connection' => env('NORMCACHE_CONNECTION', 'cache'),
    'key_prefix' => env('NORMCACHE_KEY_PREFIX', ''),
    'serializer' => env('NORMCACHE_SERIALIZER', 'auto'),

    'row_ttl' => (int) env('NORMCACHE_ROW_TTL', 604800),
    'query_ttl' => (int) env('NORMCACHE_QUERY_TTL', 3600),
    'schema_ttl' => (int) env('NORMCACHE_SCHEMA_TTL', 86400),

    // Set to 0 to disable automatic result overlays.
    'auto_overlay_max_rows' => 1000,

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

    'building_lock_ttl' => 5,
    'stampede_wait_ms' => 200,
    'stampede_wake_tokens' => 64,
    'epoch_refresh_seconds' => 5,

    'events' => (bool) env('NORMCACHE_EVENTS', false),
    'debugbar' => (bool) env('NORMCACHE_DEBUGBAR', false),
];
