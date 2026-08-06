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
Post::query()->cacheContext('tenant:' . $tenantId)->get();
```

Use the facade callback when an entire operation, including separately executed eager loads, must read directly from the database:

```php
$posts = NormCache::withoutCache(
    fn() => Post::query()->with('comments')->get(),
);
```

Writes inside the callback continue to invalidate NormCache normally.

## Canonical & Normalized Row Caching

Unlike traditional query caching, which stores a full copy of every result set, NormCache stores each row once and caches queries as references to it:

- **Rows stored once**: each database row lives under a single canonical key (`table:r:<id>`).
- **Queries store only IDs**: a normalized query stores its ordered primary keys in the `m` field of a versioned query hash (`table:q:<query_hash>`), not as copied model attributes.
- **No `KEYS` or `SCAN`**: updating a model deletes just that row key and bumps the table version counter (`table:v`). Invalidation cost does not grow with the number of cached queries.
- **One update, every query**: because all queries share the same row key, updating Post #42 refreshes it everywhere on the next read — no per-query cleanup.

## Automatic Result & Projection Overlay

For small result sets, NormCache stores the assembled result in the `r` field of the same query hash, so a warm read is a single Redis fetch instead of a membership lookup plus row assembly:

- **Automatic promotion**: a canonical query is promoted when it returns at most `auto_overlay_max_rows + 1` rows (default `1000`, so up to 1001) and the encoded payload is at most 128 KiB — a fixed cap that keeps wide rows out.
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

## Invalidation

Writes through cache-aware Eloquent or Query Builder paths invalidate automatically:

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

While disabled, reads bypass Redis and writes intentionally perform no cache invalidation. After every node runs the new code, `normcache:enable` advances the global epoch before clearing the disabled flag, making all payloads from before or during the transition unreachable.

## Configuration

```php
return [
    'enabled' => true,
    'connection' => 'cache',
    'key_prefix' => '',
    'serializer' => 'auto', // auto, php, or igbinary

    'row_ttl' => 604800,
    'query_ttl' => 3600,
    'schema_ttl' => 86400,
    // Set to 0 to disable automatic result overlays.
    'auto_overlay_max_rows' => 1000,

    'max_precise_invalidation_keys' => 1000,
    'building_lock_ttl' => 5,
    'stampede_wait_ms' => 200,
    'stampede_wake_tokens' => 64,

    'events' => false,
    'debugbar' => false,
];
```

`row_ttl` applies to shared canonical rows. `query_ttl` applies to memberships and result payloads. Per-query `ttl()` changes only query-shaped payloads. `schema_ttl` persists view, primary-key, and referential-action discovery in Redis across application requests; set it to `0` to disable persistence.

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

When an application mutates a connection in place for tenant or shard switching, update `normcache_scope` together with the database endpoint. Connections with neither a name nor an explicit scope bypass caching because a stable source identity cannot be established.

## Bypasses and limitations

NormCache bypasses reads when correctness cannot be established, including:

- open database transactions;
- `lockForUpdate()` and `sharedLock()`;
- `useWritePdo()`;
- custom fetch modes, `pretend()`, cursors, and `explain()`;
- explicit `withoutCache()`;
- raw or opaque dependencies not fully authorized with `dependsOn()`;
- volatile SQL expressions.

Canonical storage keys each row by one primary-key value, so it requires a single-column primary key. When a table has a primary key column that introspection cannot discover, name it with a `primary_keys` override to restore canonical storage.

For intercepted deletes, NormCache discovers `CASCADE`, `SET NULL`, and `SET DEFAULT` foreign-key actions through Laravel's schema API and broadly invalidates affected child tables, including multi-level cascades. The graph is rebuilt during schema refresh and persisted with the other schema metadata. A delete encountering a cold graph conservatively advances the global epoch before warming it for subsequent deletes.

Direct database writes executed outside of Eloquent or the cache-aware Query Builder, such as raw connection SQL or writes from external services, are not intercepted. Trigger side effects and `ON UPDATE` referential actions are not inferred. Declare the affected tables with `dependsOn()` on every cached read whose result can change, or call `NormCache::invalidate(...)` / `NormCache::flushAll()` after the write.

If connection schemas or table definitions change at runtime, call `NormCache::refreshSchema($connection)`.

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

When `events` is enabled, NormCache dispatches cache hit, miss, bypass, repair, and invalidation events. When `fruitcake/laravel-debugbar` is installed and `debugbar` is enabled, cache activity appears in Laravel Debugbar.

## Serializer configuration

Choose the payload serializer with `serializer` / `NORMCACHE_SERIALIZER`:

- `auto` preserves automatic extension detection;
- `php` always uses PHP serialization and is the safest choice for heterogeneous fleets;
- `igbinary` requires `ext-igbinary` on the node and fails application boot when it is unavailable.

New payloads carry a one-byte serializer marker, so an igbinary-enabled node can read both PHP and igbinary payloads during a rolling deployment. A node without igbinary treats an igbinary payload as a cache miss instead of attempting the wrong decoder.

## License

**MIT**
