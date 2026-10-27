# Laravel NormCache

**Redis-backed query caching for Laravel Eloquent.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

NormCache stores model rows once and shares them across cached queries. When data changes, it invalidates cached results through Redis counters rather than scanning and deleting keys.

**Requirements:** PHP 8.2+, Laravel 12/13, Redis 6.0+.

## Install

```bash
composer require kai-init/laravel-normcache
```

Add the `Cacheable` trait to models you want to cache:

```php
use NormCache\Traits\Cacheable;

class Post extends Model
{
    use Cacheable;
}
```

Configuration is optional. To customize it:

```bash
php artisan vendor:publish --tag=normcache-config
```

## Use

Queries work as usual. No special cache calls are needed:

```php
Post::where('published', true)->get();
Post::find(1);
```

- **Shared rows:** multiple queries reuse the same cached model rows.
- **Partial hits:** only missing or expired rows are fetched from the database.
- **Automatic invalidation:** writes through `Cacheable` models and builders update cache versions.
- **Safe fallback:** unsupported queries or unavailable Redis fall back to SQL.

**Important:** Only models using `Cacheable` are observed. After writes through `DB::table()`, raw SQL, triggers, or other models, invalidate the affected tables yourself:

```php
use NormCache\Facades\NormCache;

NormCache::invalidate([Post::class]);
```

## Laravel Debugbar

NormCache includes a **Debugbar panel** showing cache hits, misses, repairs, bypasses, and invalidations, with timings and SQL details.

```bash
composer require --dev fruitcake/laravel-debugbar
```

Set `NORMCACHE_DEBUGBAR=true` in `.env`. The **NormCache** panel appears when Laravel Debugbar is enabled.

## More details

<details>
<summary><b>Query options and tags</b></summary>

```php
Post::query()->where('published', true)->ttl(600)->get();
Post::query()->tag('homepage')->get();
Post::query()->withoutCache()->get();

NormCache::flushTag('homepage');
```

`ttl()` changes the query result TTL. `tag()` groups query results for selective flushing; flushing a tag advances a counter without scanning Redis. Table writes still invalidate tagged results automatically.

To bypass caching for an entire operation, including separate eager-load queries:

```php
NormCache::withoutCache(fn() => Post::with('comments')->get());
```

Writes in that callback still invalidate normally.

</details>

<details>
<summary><b>Security contexts and query dependencies</b></summary>

If row visibility depends on an implicit database policy (such as PostgreSQL row-level security, session roles, or tenant session variables), provide a stable context:

```php
Post::query()
    ->cacheContext('tenant:' . $tenantId)
    ->get();
```

Contexts are hashed into the query identity and use isolated full-result caching. Apply the context to *every* affected query, including eager loads. If you cannot provide a stable context, use `withoutCache()`. Tenant filters already present in SQL bindings do not need an extra context.

NormCache detects common joins, subqueries, and relationships. For SQL views or opaque raw SQL, declare all underlying physical tables:

```php
AuthorReport::query()
    ->dependsOn([Author::class, Post::class])
    ->get();
```

`dependsOn()` accepts model classes or table names. Missing dependencies can leave cached results stale, so list every table the query depends on.

</details>

<details>
<summary><b>How caching works and when it is bypassed</b></summary>

For ordinary single-table queries, NormCache caches ordered primary keys and shares the actual rows:

1. On a cold miss, it queries IDs, then loads only missing rows (batches of up to 900, or 5,000 on MySQL/MariaDB).
2. On a warm read, it fetches shared rows with a bulk Redis `MGET`, checks cache versions, and restores query order, including repeated IDs.
3. Small results can also be stored as inline overlays for faster warm reads (up to 1,001 rows by default; 32 KiB for plain queries, 128 KiB for tagged or dependency-bearing queries).

Selected columns, aggregates, joins, raw ordering, and explicitly dependent queries use full-result caching instead. A direct primary-key miss needs only one SQL query. Reusing or repairing rows does not extend their TTL or the query's lifetime. If a fill becomes stale, NormCache runs the original SQL.

Reads bypass the cache when safety cannot be established, including open transactions, locks, `useWritePdo()`, custom fetch modes, `pretend()`, cursors, `explain()`, and unsupported raw dependencies.

Opt out explicitly for random, time-dependent, or session-dependent results; NormCache cannot detect every such query:

```php
Post::query()->withoutCache()->inRandomOrder()->first();
```

Shared-row caching requires a declared single-column model primary key.

</details>

<details>
<summary><b>Invalidation, Redis outages, and deployments</b></summary>

Writes through `Cacheable` models invalidate automatically, including after the outer transaction commits. Deletes account for foreign-key side effects. For external writes or database triggers, invalidate every affected table:

```php
NormCache::invalidate([Post::class, Comment::class], connection: 'mysql');
NormCache::invalidate(['posts', 'comments'], connection: 'mysql');
NormCache::flushAll();
```

Laravel migrations trigger a global flush. After other schema changes, call `flushAll()` and restart long-running workers if they must see the change immediately.

For a global flush or a rolling deployment:

```bash
php artisan normcache:flush
php artisan normcache:disable
# Deploy to every web and worker node.
php artisan normcache:enable
```

Disabling bypasses cached reads and suspends write invalidation. Enabling advances the global epoch so previously cached data cannot be reused. Keep caching disabled until every node is updated when deploying changes to the cache key format.

**Redis outage warning:** NormCache fails open to SQL, but if one writer loses Redis while other nodes can still read it, those nodes may serve stale entries. Failures are logged; after Redis is restored, run `php artisan normcache:flush`.

</details>

<details>
<summary><b>Configuration and Redis setup</b></summary>

All defaults are in [`config/normcache.php`](config/normcache.php). Common options:

| Option | Default | Purpose |
| --- | --- | --- |
| `connection` | `cache` | Laravel Redis connection |
| `row_ttl` | `604800` | Shared-row lifetime (seconds) |
| `query_ttl` | `3600` | Query-result lifetime (seconds) |
| `auto_overlay_max_rows` | `1000` | Maximum overlay size; `0` disables overlays |
| `serializer` | `auto` | `auto`, `php`, or `igbinary` |
| `epoch_refresh_seconds` | `5` | Global-state refresh interval |
| `events` / `debugbar` | `false` | Diagnostics and optional Debugbar panel |

Choose `serializer=php` for mixed fleets; `igbinary` requires the PHP extension. When enabled, events report hits, misses, bypasses, repairs, and invalidations.

By default, table caches are isolated by Laravel connection name. Set `normcache_scope` in a database connection's configuration when different connection aliases intentionally point to the same physical data source. Unnamed connections need an explicit scope.

**Redis memory:** Invalidated payloads expire by TTL. Use a memory limit and a `volatile-*` eviction policy so Redis can evict payloads without evicting the persistent version counters:

```ini
maxmemory 4gb
maxmemory-policy volatile-lru
```

Redis Cluster is supported. Table keys share hash slots; global and dependency state is validated separately.

</details>

## License

[MIT](LICENSE)
