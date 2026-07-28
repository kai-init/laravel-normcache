# Laravel NormCache

**Redis-backed normalized query caching for Laravel Eloquent and Query Builder.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

NormCache stores complete model rows once and lets many cached queries share them. Writes bumps Redis counters instead of scanning and deleting every query that might contain a changed row.

Requirements: PHP 8.2+, Laravel 12/13, Redis 6.0+.

## Installation

```bash
composer require kai-init/laravel-normcache
php artisan vendor:publish --tag=normcache-config
```

Add `Cacheable` to Eloquent models whose writes and reads NormCache should observe:

```php
use NormCache\Traits\Cacheable;

class Post extends Model
{
    use Cacheable;
}
```

The service provider also installs cache-aware Laravel database connections, so supported `DB::table()` reads are cached automatically. `DB::select()` and other raw connection calls are not intercepted.

## Usage

Ordinary reads need no cache-specific call:

```php
Post::where('published', true)->get();
Post::find(1);
DB::table('posts')->where('published', true)->orderBy('id')->get();
```

Query controls are available on Eloquent and Query Builder:

```php
Post::query()->withoutCache()->get();
Post::query()->where('published', true)->ttl(600)->get();
Post::query()->where('published', true)->tag('homepage')->get();
Post::query()->orderBy('id')->useResultCache()->get();
```

`useResultCache()` materializes a complete result payload in addition to normalized canonical storage. It is useful for repeatedly reading large result sets when model sharing is less important than avoiding row-by-row payload assembly.

Aggregates, `exists`, `value`, `pluck`, pagination totals, relationship eager loading, and supported relationship aggregates use the same planner and safety checks.

## Dependencies

NormCache infers identifiable tables from ordinary joins, unions, subqueries, and relationship queries. If a query contains an opaque expression or source, declare every table it reads:

```php
Author::query()
    ->whereRaw('exists (select 1 from legacy_stats where legacy_stats.author_id = authors.id)')
    ->dependsOn([Post::class, 'legacy_stats'])
    ->get();
```

`dependsOn()` accepts Eloquent model classes and table names. It authorizes an otherwise opaque query only when NormCache can resolve all declared dependencies. Volatile expressions such as random, UUID, clock, connection-state, or sleep functions are never cached.

## Invalidation

Writes through cache-aware Eloquent or Query Builder paths invalidate automatically. Invalidations inside a transaction are applied only after the outer transaction commits.

Use the facade after writes performed elsewhere:

```php
use NormCache\Facades\NormCache;

NormCache::invalidateTable('mysql', 'posts');
NormCache::invalidateTables('mysql', ['posts', 'comments']);
NormCache::flushTag('homepage');
NormCache::flushAll();
```

The global CLI flush takes no options:

```bash
php artisan normcache:flush
```

`flushAll()` and the command advance a global epoch. Old payloads expire naturally; NormCache does not scan Redis keys.

## Configuration

```php
return [
    'enabled' => true,
    'connection' => 'cache',
    'key_prefix' => '',

    'row_ttl' => 604800,
    'query_ttl' => 3600,

    'max_precise_invalidation_keys' => 1000,
    'building_lock_ttl' => 5,
    'stampede_wait_ms' => 200,
    'stampede_wake_tokens' => 64,

    'events' => false,
    'debugbar' => false,
];
```

`row_ttl` applies to shared canonical rows. `query_ttl` applies to memberships and result payloads. Per-query `ttl()` changes only query-shaped payloads.

For tables whose primary key cannot be discovered reliably, configure grouped overrides:

```php
'primary_keys' => [[
    'connection' => 'pgsql',
    'database' => 'app',
    'schema' => 'public',
    'tables' => [
        'events' => ['column' => 'event_id', 'type' => 'string'],
        'orders' => ['column' => 'order_id', 'type' => 'integer'],
    ],
]],
```

## Bypasses and limitations

NormCache bypasses reads when correctness cannot be established, including:

- open database transactions;
- `lockForUpdate()` and `sharedLock()`;
- `useWritePdo()`;
- custom fetch modes, `pretend()`, cursors, and `explain()`;
- explicit `withoutCache()`;
- raw or opaque dependencies not fully authorized with `dependsOn()`;
- volatile SQL expressions.

Canonical storage requires a supported single-column integer or string primary key. Queries can still use `result` storage when canonical routing is unavailable.

Writes performed through raw SQL or a connection not installed by NormCache are invisible until `invalidateTable()`, `invalidateTables()`, or `flushAll()` is called. After changing connection database/schema metadata at runtime, call `NormCache::clearSchemaMetadata()` for that connection.

## Redis Cluster

All keys for one physical table share a Redis hash slot. Query-group entries use their own query hash slot. Global epoch, dependency versions, and tag versions are read separately and validated against payload state; Predis Cluster batches cross-slot state groups in one pipeline.

## Observability

When `events` is enabled, NormCache dispatches cache hit, miss, bypass, repair, and invalidation events. When `fruitcake/laravel-debugbar` is installed and `debugbar` is enabled, cache activity appears in Laravel Debugbar.

## License

MIT
