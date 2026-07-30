# Laravel NormCache

**Redis-backed normalized query caching for Laravel Eloquent and Query Builder.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

NormCache stores complete model rows once and lets many cached queries share them. Invalidation is `O(1)`: a write bumps Redis counters instead of scanning and deleting every query that might contain a changed row.

Requirements: PHP 8.2+, Laravel 12/13, Redis 6.0+.

## Installation

```bash
composer require kai-init/laravel-normcache
```

Publish the configuration:

```bash
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

Cache controls are available on Eloquent and Query Builder:

```php
Post::query()->withoutCache()->get();
Post::query()->where('published', true)->ttl(600)->get();
Post::query()->where('published', true)->tag('homepage')->get();
```

## Canonical & Normalized Row Caching

Unlike traditional query caching, which stores a full copy of every result set, NormCache stores each row once and caches queries as references to it:

- **Rows stored once**: each database row lives under a single canonical key (`table:r:<id>`).
- **Queries store only IDs**: a cached query is an ordered list of primary keys (`table:m:<query_hash>`), not a copy of the model attributes.
- **No `KEYS` or `SCAN`**: updating a model deletes just that row key and bumps the table version counter (`table:v`). Invalidation cost does not grow with the number of cached queries.
- **One update, every query**: because all queries share the same row key, updating Post #42 refreshes it everywhere on the next read — no per-query cleanup.

## Automatic Result & Projection Overlay

For small result sets, NormCache also stores the assembled result alongside the canonical rows, so a warm read is a single Redis fetch instead of a membership lookup plus row assembly:

- **Automatic promotion**: a canonical query is promoted when it returns at most `auto_overlay_max_rows + 1` rows (default `50`, so up to 51) and the encoded payload is under 50 KiB.
- **Self-healing**: writes to the query's tables invalidate the overlay along with the canonical rows. If the overlay is missing or expired, the read falls back to canonical row assembly and re-promotes.

## Tags and selective flushing

Use `tag()` to group related cached queries under a named invalidation namespace:

```php
$posts = Post::query()
    ->where('published', true)
    ->tag('homepage')
    ->get();
```

Any number of different queries can share the same tag, and flushing that tag invalidates all of their query-shaped payloads without affecting untagged queries or queries using another tag:

```php
use NormCache\Facades\NormCache;

NormCache::flushTag('homepage');
```

`flushTag()` advances a Redis version counter; it does not scan for or delete matching keys. The affected queries miss and rebuild on their next read, while old payloads expire naturally. Tags are an additional manual invalidation boundary and do not replace automatic dependency invalidation when an underlying table changes.

## Dependencies

NormCache infers identifiable tables from ordinary joins, unions, subqueries, and relationship queries. If a query contains an opaque expression or source, declare every table it reads:

```php
Author::query()
    ->whereRaw('exists (select 1 from legacy_stats where legacy_stats.author_id = authors.id)')
    ->dependsOn([Post::class, 'legacy_stats'])
    ->get();
```

`dependsOn()` accepts Eloquent model classes and table names. It authorizes an otherwise opaque query only when NormCache can resolve all declared dependencies. Recognized volatile expressions—including random, UUID, clock, connection-state, sequence-state, and sleep functions—are never cached.

## Invalidation

Writes through cache-aware Eloquent or Query Builder paths invalidate automatically. Invalidations inside a transaction are applied only after the outer transaction commits.

Use the facade after writes performed elsewhere:

```php
NormCache::invalidate([Post::class, Comment::class], connection: 'mysql');
NormCache::invalidate(['posts', 'comments'], connection: 'mysql');
NormCache::flushTag('homepage');
NormCache::flushAll();
```

The global CLI flush takes no options:

```bash
php artisan normcache:flush
```

`flushAll()` and the command advance a global epoch. Old payloads expire naturally; NormCache does not scan Redis keys.

## Redis invalidation outages

NormCache fails open when Redis is unavailable: the database write succeeds and the affected request bypasses cache access. If only that writer cannot reach Redis while other application nodes can still read it, those nodes can serve stale cached data until the affected entry expires or invalidation later succeeds. NormCache logs this condition at `critical` level with the affected table and invalidation mode.

After Redis connectivity is restored, run the global flush to advance the epoch and make any payloads from the outage unreachable:

```bash
php artisan normcache:flush
```

## Temporarily disabling the cache

Use the runtime commands when NormCache needs to be paused across all application nodes without changing configuration or redeploying:

```bash
php artisan normcache:disable
php artisan normcache:enable
```

While disabled, reads bypass NormCache and go directly to the database, and writes do not perform cache invalidation. The switch is stored in Redis and is observed by new requests and jobs across every node.

`normcache:enable` atomically advances the global epoch before clearing the disabled flag. This prevents payloads cached before the pause from being served after writes occurred while invalidation was disabled.

## Configuration

```php
return [
    'enabled' => true,
    'connection' => 'cache',
    'key_prefix' => '',

    'row_ttl' => 604800,
    'query_ttl' => 3600,
    // Set to 0 to disable automatic result overlays. Admission allows this value plus one row for pagination lookahead; encoded overlays are capped at 50 KiB.
    'auto_overlay_max_rows' => 50,

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

Direct database writes executed outside of Eloquent (such as raw SQL, triggers, or external services) bypass automatic cache interception. Use `NormCache::invalidate(...)` or `NormCache::flushAll()` to manually invalidate affected models or tables. If connection schemas or table definitions are modified at runtime, call `NormCache::clearSchemaMetadata($connection)` to reset cached schema metadata.

## Redis Cluster

All keys for one physical table share a Redis hash slot. Query-group entries use their own query hash slot. Global epoch, dependency versions, and tag versions are read separately and validated against payload state; Predis Cluster batches cross-slot state groups in one pipeline.

## Observability

When `events` is enabled, NormCache dispatches cache hit, miss, bypass, repair, and invalidation events. When `fruitcake/laravel-debugbar` is installed and `debugbar` is enabled, cache activity appears in Laravel Debugbar.

## Optional igbinary serialization

When the `ext-igbinary` PHP extension is available, NormCache detects it automatically and uses it for cached payloads. Otherwise it falls back to PHP's native serialization; no configuration is required.

Every application node and worker sharing the same Redis cache must use the same serializer. After installing or removing igbinary, run `php artisan normcache:flush` before serving traffic so payloads written with the previous format are not reused.

## License

MIT
