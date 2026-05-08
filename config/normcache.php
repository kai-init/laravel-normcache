<?php

return [
    'connection'    => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled'       => env('NORMCACHE_ENABLED', true),
    'ttl'           => (int) env('NORMCACHE_TTL', 604800),
    'query_ttl'     => (int) env('NORMCACHE_QUERY_TTL', 3600),
    'key_prefix'    => env('NORMCACHE_PREFIX', ''),
    'cooldown'      => (int) env('NORMCACHE_COOLDOWN', 0),
    'cluster'       => (bool) env('NORMCACHE_CLUSTER', false),
    'events'        => (bool) env('NORMCACHE_EVENTS', true),
];
