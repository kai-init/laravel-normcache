# Laravel NormCache

**Redis-backed normalized query caching for Laravel Eloquent.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

NormCache stores complete model rows once and lets many cached queries share them. Writes bump Redis counters instead of scanning and deleting every query that might contain a changed row.

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

NormCache only watches models using `Cacheable`. After writes through `DB::table()`, raw SQL, or other models, call `NormCache::invalidate()` for the affected tables.

## Usage

Ordinary reads need no cache-specific call:

```php
Post::where('published', true)->get();
Post::find(1);
```

Cache controls are available on Cacheable Eloquent queries and their `toBase()` builders:

```php
Post::query()->withoutCache()->get();
Post::query()->where('published', true)->ttl(600)->get();
Post::query()->where('published', true)->tag('homepage')->get();
Post::query()->cacheContext('tenant:' . $tenantId)->get();
```

Use the facade callback when an entire operation, including separately executed eager loads, must read directly from the database:

```php
$posts = NormCache::withoutCache(
    fn() => Post::query()->with('comments')->get(),
);
```

Writes inside the callback continue to invalidate NormCache normally.

## Shared row caching

Unlike traditional query caching, which stores a full copy of every result set, NormCache stores each row once and caches queries as references to it:

- **Shared rows**: queries store ordered IDs and reuse the same cached rows.
- **Partial hits**: if some rows are missing, NormCache fetches just those rows.
- **No scans**: writes invalidate rows and queries through version counters, without searching Redis keys.

On a query miss, NormCache fetches IDs first, then any missing rows in batches of 900. That usually means two SQL queries, or just one if all rows are already cached. A direct `find()` miss still takes one query.

If data changes during a fill or repair, NormCache falls back to the original query. Repairing rows does not extend the cached query's lifetime.

## Result caching

Small results also get a full-result “overlay,” so warm reads need only one Redis fetch:

- Overlays are created during fills, up to `auto_overlay_max_rows + 1` rows (1001 by default) and 128 KiB.
- Missing overlays use shared rows instead. Cache hits do not recreate them.

Queries with selected columns, aggregates, joins, raw ordering, or explicit dependencies cache their full results instead of sharing rows. On a miss, they run their original SQL once.

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
NormCache::flushTag('homepage');
```

`flushTag()` advances a Redis version counter; it does not scan for or delete matching keys. The affected queries miss and rebuild on their next read, while old payloads expire naturally. Tags are an additional manual invalidation boundary and do not replace automatic dependency invalidation when an underlying table changes.

## Database security contexts

Queries whose results depend on implicit database state must declare a stable cache context. This includes PostgreSQL row-level security, SQL Server security policies, active database roles, and tenant-aware session variables that change row visibility without changing SQL or bindings:

```php
$posts = Post::query()
    ->cacheContext('tenant:' . $tenantId)
    ->where('published', true)
    ->get();
```

The context is hashed into the cache identity and is never written verbatim to Redis. Context-bearing queries use isolated full-result storage rather than shared canonical rows, because the same physical primary key may represent a different visible row in each database security context.

Apply `cacheContext()` to every cached query affected by the implicit policy, including separately executed eager-load queries. If a stable context is unavailable, use `withoutCache()` instead. Authorization or tenancy already represented in SQL bindings, the database source scope, or physical table identity does not need an additional cache context.

## Dependencies

NormCache infers identifiable tables from ordinary joins, unions, subqueries, and relationship queries. Complex or rejected raw SQL can still be cached with explicit physical dependencies:

```php
Author::query()
    ->whereRaw(
        'exists (select 1 from (select author_id from legacy_stats) as recent where recent.author_id = authors.id)'
    )
    ->dependsOn(['legacy_stats'])
    ->get();
```

`dependsOn()` accepts Eloquent model classes and table names. It authorizes an otherwise opaque query only when NormCache can resolve all declared dependencies.

For SQL views, declare every underlying table yourself:

```php
AuthorReport::query()->dependsOn([Author::class, Post::class])->get();
```

Use a global scope to apply this to every query on a view model. Views in joins and subqueries need declarations too; missing dependencies can leave stale results.

## Invalidation

Writes through Cacheable Eloquent models and their builders invalidate automatically:

- inside a transaction, invalidation is applied only after the outer transaction commits;
- if a write's target cannot be resolved safely, NormCache advances the global epoch rather than leave reachable stale data;
- if a write throws a database exception, its outcome is uncertain, so its tables are invalidated broadly before the original exception is rethrown.

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

NormCache fails open when Redis is unavailable: the database write succeeds and that request bypasses the cache. If only the writer loses Redis while other nodes can still read it, those nodes can serve stale data until the entry expires or a later invalidation succeeds. The condition is logged at `critical` with the affected table and invalidation mode.

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

Use this deployment sequence:

```bash
php artisan normcache:disable
# Deploy the new package version to every web and worker node.
php artisan normcache:enable
```

Cache keys have changed in this version. Keep caching disabled until every node is updated.

## Configuration

```php
return [
    'enabled' => true,
    'connection' => 'cache',
    'key_prefix' => '',
    'serializer' => 'auto', // auto, php, or igbinary

    'row_ttl' => 604800,
    'query_ttl' => 3600,
    // Set to 0 to disable automatic result overlays.
    'auto_overlay_max_rows' => 1000,

    'max_precise_invalidation_keys' => 1000,
    'building_lock_ttl' => 5,
    'stampede_wait_ms' => 200,
    'stampede_wake_tokens' => 64,
    'epoch_refresh_seconds' => 5,

    'events' => false,
    'debugbar' => false,
];
```

`row_ttl` controls shared rows; `query_ttl` and per-query `ttl()` control query results. Set `$primaryKey` and `$keyType` on your model as usual.

`epoch_refresh_seconds` controls how often running requests and jobs check for a global flush. With `0`, they check only at the start of a new scope. Schema metadata is shared through Redis for 24 hours and refreshed after a global flush.

### Database source scopes

Table cache identity includes a logical database source scope. By default NormCache uses the Laravel connection name, which keeps connections such as `shard-a` and `shard-b` isolated even when both servers use the same database and table names.

Set `normcache_scope` inside a Laravel database connection when aliases intentionally represent the same physical data source:

```php
'connections' => [
    'mysql-primary' => [
        'driver' => 'mysql',
        'normcache_scope' => 'commerce-primary',
        // ...
    ],

    'mysql-read' => [
        'driver' => 'mysql',
        'normcache_scope' => 'commerce-primary',
        // ...
    ],
],
```

Aliases with the same scope share table caches, including when they spell the same table with different Laravel prefixes. When switching tenants or shards, update the scope along with the connection. Unnamed connections need an explicit scope to use caching.

## Bypasses and limitations

NormCache bypasses reads when correctness cannot be established, including:

- open database transactions;
- `lockForUpdate()` and `sharedLock()`;
- `useWritePdo()`;
- custom fetch modes, `pretend()`, cursors, and `explain()`;
- explicit `withoutCache()`;
- raw or opaque dependencies not fully authorized with `dependsOn()`.

Use `withoutCache()` for random, time-dependent, or session-dependent queries. NormCache does not detect these for you:

```php
$sample = Post::query()->withoutCache()->inRandomOrder()->first();
```

Shared row caching requires a single-column primary key declared on the model.

Deletes also invalidate tables affected by `CASCADE`, `SET NULL`, and `SET DEFAULT` foreign keys. If schema lookup fails, NormCache flushes globally.

For external writes, triggers, and `ON UPDATE` side effects, invalidate affected tables yourself. For intercepted writes, `dependsOn()` can link affected queries to the table that triggers the change.

Laravel migrations flush the cache automatically. After other schema changes, call `NormCache::flushAll()`. Restart long-running workers if they need to pick up the change immediately.

## Redis Cluster

All keys for one physical table share a Redis hash slot. Query-group entries use their own query hash slot. Global epoch, dependency versions, and tag versions are read separately and validated against payload state; Predis Cluster batches cross-slot state groups in one pipeline.

## Recommended Redis Configuration

Broad invalidation (generation bumps, tag flushes, epoch advances) retires entries by bumping a counter rather than deleting keys, so orphaned payloads stay in Redis until `row_ttl` / `query_ttl` expires them. Give Redis a memory ceiling and a `volatile-*` eviction policy:

```ini
maxmemory 4gb
maxmemory-policy volatile-lru
```

`volatile-*` evicts only keys that carry a TTL. NormCache's payloads do; its version counters do not, so they survive eviction — which matters, because losing a counter would make already-retired payloads readable again.

## Observability

Enable `events` for hit, miss, bypass, repair, and invalidation events. Enable `debugbar` with `fruitcake/laravel-debugbar` installed to see cache activity in Laravel Debugbar.

## Serializer configuration

Choose the payload serializer with `serializer` / `NORMCACHE_SERIALIZER`:

- `auto` preserves automatic extension detection;
- `php` always uses PHP serialization and is the safest choice for heterogeneous fleets;
- `igbinary` requires `ext-igbinary` on the node and fails application boot when it is unavailable.

New payloads carry a one-byte serializer marker, so an igbinary-enabled node can read both PHP and igbinary payloads during a rolling deployment. A node without igbinary treats an igbinary payload as a cache miss instead of attempting the wrong decoder.

Rows with stream values, such as PostgreSQL `bytea`, skip caching and are returned unchanged. Binary strings can still be cached.

## License

**MIT**
