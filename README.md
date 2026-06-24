# Laravel Normcache

**Normalized caching for Laravel Eloquent. Self-invalidating, Redis-backed.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

Most caching packages store each query result as one serialized collection. Normcache takes a different approach: a query cache only stores the matching IDs, while each model's attributes live in their own key. The same model can appear in many cached queries but is only stored once, so a single version bump invalidates everything that returned it, in O(1).

```
query:{posts}:v3:...  →  [4, 7, 12]
model:{posts}:4       →  { id:4, title:..., body:... }
model:{posts}:7       →  { id:7, title:..., body:... }
model:{posts}:12      →  { id:12, title:..., body:... }
```

**Requirements:** PHP 8.2+, Laravel 12/13, Redis 4.0+

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Cache Bypasses](#cache-bypasses)
- [Limitations](#limitations)
- [Configuration](#configuration)
- [Observability](#observability)
- [Redis Clustering](#redis-clustering)
- [Octane & Horizon](#octane--horizon)
- [Performance](#performance)
- [License](#license)

---

## What's New in v2

Version 2 extends Normcache beyond normalized caching into a full read-path cache layer:

- **`dependsOn([Model::class])`** and **`dependsOnTables(['table'])`** — cache cross-table queries by declaring what should invalidate them. Simple cases stay normalized; complex shapes use a versioned result cache.
- **Scalar and aggregate caching** — `count`, `sum`, `avg`, `withCount`, `withSum`, and friends are cached automatically under versioned keys.
- **Stampede protection** — waiters serve stale data or block on a wake channel instead of storming the database during a rebuild.
- **Redis Cluster support** — single-slot mode by default; opt into per-model slot sharding with `slotting`.
- **Tag-based flushing** — group query entries under a tag and flush them together on deploy or config change.
- **Debugbar integration** — hits, misses, and bypasses appear on the request timeline when Debugbar is installed.

---

## Installation

```bash
composer require kai-init/laravel-normcache
```

Add the `Cacheable` trait to any model you want cached:

```php
use NormCache\Traits\Cacheable;

class Post extends Model
{
    use Cacheable;
}
```

---

## Usage

### Basic Queries

```php
Post::all();
Post::where('active', true)->get();
Post::find(1);
Post::paginate(20);
```

### Bypassing the Cache

```php
Post::withoutCache()->get();
```

### Cross-Table Queries

Two shapes are inferred automatically — no `dependsOn()` needed:

- `whereHas` / `whereDoesntHave` on a non-nested `Cacheable` relation
- plain string `join()` calls with an explicit root-table projection

Join inference is conservative and falls back to requiring `dependsOn()`/`dependsOnTables()` for:

- implicit aliases, e.g. `join('posts p', ...)`
- expression join targets
- raw or subquery join predicates
- other unsupported join-clause conditions

```php
Author::whereHas('posts', fn($q) => $q->where('published', true))->get();

Author::join('posts', 'posts.author_id', '=', 'authors.id')
    ->select('authors.*')
    ->get(); // also works with count(), sum(), exists(), paginate()
```

Everything else requires `dependsOn()`, including:

- manual `whereExists`
- raw predicates
- nested relations
- `GROUP BY` / `DISTINCT`
- calculated columns

Use `dependsOnTables()` when the joined table has no `Cacheable` model:

```php
Author::join('legacy_stats', 'legacy_stats.author_id', '=', 'authors.id')
    ->select('authors.*')
    ->dependsOnTables(['legacy_stats'])
    ->get();
```

> **Note:** `dependsOnTables()` declares a read dependency only. Call `NormCache::invalidateTableVersion('mysql', 'legacy_stats')` after any external write to that table.

Normcache chooses the best caching strategy automatically:

- **Normalized Cache**: Used for simple queries on the primary table. If you add `dependsOn()`, it stays normalized but becomes versioned against the extra models too.
- **Result Cache**: Used for complex queries with `dependsOn()`. The entire result set is cached as a versioned blob.

Pessimistic locks always bypass the cache.

### Per-Query TTL

Use `ttl()` to set a custom cache duration:

```php
Post::where('active', true)->ttl(600)->get();
```

### Aggregates

`withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached automatically. The result set is cached as a single versioned blob and invalidated when any related model version changes.

```php
Post::withCount('comments')->get();
Post::withoutAggregateCache()->withCount('comments')->get(); // skip aggregate cache
```

### Relationship Caching

`BelongsTo`, `BelongsToMany`, `MorphTo`, `MorphToMany`, `MorphedByMany`, `HasManyThrough`, and `HasOneThrough` are cached for eager loads — on a warm hit no SQL is executed. `HasOne`, `HasMany`, `MorphOne`, and `MorphMany` are cached via the query cache when the related model uses `Cacheable`.

`attach`, `detach`, `sync`, and `updateExistingPivot` automatically invalidate the relevant pivot cache.

### Manual Flush

```bash
php artisan normcache:flush --model="App\Models\Post"
php artisan normcache:flush
```

```php
NormCache::flushModel(Post::class);
NormCache::flushAll();
```

If you mutate cacheable tables outside Eloquent, flush manually after the write:

```php
DB::table('posts')->update(['published' => true]);
NormCache::flushModel(Post::class);
```

### Tag-Based Flush

Tag any query to group cache entries for manual flushing — useful for invalidation events the version system can't see (deploys, config changes, nightly rebuilds). Tags must not contain `: { } *` or whitespace.

```php
Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

NormCache::flushTag(Author::class, 'homepage');   // single model — single-slot scan
NormCache::flushTagAcrossModels('homepage');       // all models — cluster-wide scan
```

---

## Cache Bypasses

To prevent data corruption, NormCache will automatically bypass caching and fall back to the database for:

| Query feature                                        | Workaround            |
| ---------------------------------------------------- | --------------------- |
| Pessimistic locking (`lockForUpdate` / `sharedLock`) | None — must hit DB    |
| Inside a database transaction                        | None — must hit DB    |
| Raw SQL / `DB::table(...)`                           | None — flush manually |
| Raw `WHERE` or `ORDER BY` clauses                    | Use `dependsOn()`     |
| Cross-table aggregate and scalar queries             | Use `dependsOn()`     |
| `chunk()`, `each()`, `lazy()`                        | None — always hits DB |
| `sole()`                                             | None — always hits DB |

Everything else — `JOIN`, `GROUP BY`, `DISTINCT`, subquery `WHERE`, and calculated columns — is also cacheable with `dependsOn()`.

---

## Limitations

- Normcache only hooks Eloquent models that use the `Cacheable` trait. Query builder calls such as `DB::table(...)`, `DB::select()`, and `DB::statement()` are never cached.
- Writes outside Eloquent are invisible to the model version system. Flush the affected model or tag manually after imports, raw updates, maintenance jobs, or external syncs.
- Normcache caches each model's connection name and table in static properties for performance. Call `CacheKeyBuilder::reset()` after switching tenants to clear the metadata cache.
- `dependsOn()` is explicit by design. If a query reads another table, include that model class or manually flush a tag that covers the query.
- Models are expected to use standard single-column primary keys.
- Packages that replace Eloquent builders, relation classes, or hydration behavior may bypass parts of Normcache.

---

## Configuration

Publish `config/normcache.php` to tune these options (each is also configurable via a `NORMCACHE_*` env var):

- **`connection`** — Redis connection (from `config/database.php`) NormCache reads and writes through. Default: `cache`.
- **`enabled`** — Master switch. When `false`, every query falls through to the database. Default: `true`.
- **`ttl`** — Lifetime of individual model attribute keys. Default: 7 days.
- **`query_ttl`** — Lifetime of query, raw, pivot, and through cache keys. Default: 1 hour.
- **`key_prefix`** — String prepended to every NormCache Redis key — use to namespace shared Redis instances. Default: none.
- **`slotting`** — When `false` (default), all NormCache keys are placed on one Redis Cluster slot using the `{nc}` slot prefix.
- **`cooldown`** — Useful for write-heavy models. Version bump debounce in seconds. Manual calls to `NormCache::flushModel()` always invalidate immediately regardless of this setting.
- **`building_lock_ttl`** — How long a cache-build lock is held before it expires and another request can take over.
- **`stampede_wait_ms`** — How long a waiter blocks on a wake channel before falling back to the database. Requires Redis 6.0+ for sub-second precision.
- **`stale_version_depth`** — How many old query-cache versions to serve as stale data while a rebuild is in progress. Set to `0` to disable stale serving.
- **`cluster`** — Enable Redis Cluster-aware key routing and multi-slot Lua calls. Default: `false`.
- **`events`** — Set to `false` to skip hit/miss event dispatches on hot paths.
- **`fallback`** — When `true`, Redis exceptions disable the cache for the request and queries fall back to the database silently.
- **`fire_retrieved`** — When `true`, models hydrated from Redis fire Eloquent's `retrieved` event.
- **`debugbar`** — Enable the Laravel Debugbar collector (see Observability). Default: `false`.

---

## Observability

### Laravel Debugbar

When [`fruitcake/laravel-debugbar`](https://github.com/fruitcake/laravel-debugbar) is installed, enable the Normcache collector:

```php
'debugbar' => env('NORMCACHE_DEBUGBAR', false),
```

This adds a **Normcache** timeline tab showing every query hit, miss, bypass, and model fetch — with key, kind, and duration — for the current request.

### Events

| Event            | Fired when                               | Properties            |
| ---------------- | ---------------------------------------- | --------------------- |
| `QueryCacheHit`  | Cached query result served from Redis    | `modelClass`, `key`   |
| `QueryCacheMiss` | Query not cached — DB queried            | `modelClass`, `key`   |
| `ModelCacheHit`  | Model attributes served from Redis       | `modelClass`, `ids[]` |
| `ModelCacheMiss` | Model attributes not cached — DB queried | `modelClass`, `ids[]` |

---

## Redis Clustering

By default, Redis Cluster support uses single-slot mode. With `cluster` enabled and `slotting` disabled, every NormCache key is prefixed with `{nc}:`, so cross-model operations can keep version checks, reads, and build-lock acquisition in one single-slot Lua command.

```php
'cluster' => true,
'slotting' => false, // default
```

Set `slotting` to `true` only when you want Redis Cluster slot sharding across model groups. In sharded mode, single-model operations keep keys on one slot via per-model hash tags (`{posts}`, `{analytics:posts}`). Cross-model operations (`dependsOn`, pivot, through, `withCount`) resolve each model's version key with separate single-slot Lua calls, then read or write on the primary model's slot.

**Consistency note:** sharded cross-model version resolution is not atomic. A writer that bumps a dependency version between version reads may cause stale response before the next request uses the new version. This is the same eventually-consistent trade-off accepted by most distributed caches.

`flushAll()` is supported.

---

## Octane & Horizon

Works out of the box. State is reset between Octane requests and queue jobs — including re-enabling the cache if a Redis error disabled it mid-job.

---

## Correctness Guarantees

NormCache is designed to be as transparent as possible to native Eloquent, but it operates under specific guarantees and intentional limitations:

### Safe & Transparent Caching

NormCache matches native Eloquent hydration for supported query shapes, with the following intentional limitations:

- **Universal Query Patterns:** Standard model lookups, primary key fast-paths, and complex result sets across both Normalized and Result modes.
- **Full Relationship Support:** Eager-loaded relations including nested chains, pivot table attributes, and through-relations.
- **Native Model Lifecycle:** Full support for standard Eloquent behavior including global scopes and soft deletes. `retrieved` events fire only when `fire_retrieved` is enabled (see Configuration).
- **Eloquent Extensibility:** Custom casts (JSON/Enum) and custom collection classes. Cached hydration reconstructs models from raw attributes directly and does not invoke custom `newFromBuilder()` overrides.

### Requires `dependsOn()`

Simple `whereHas` and plain `join()` with an explicit root-table projection are inferred automatically. Everything else — manual `whereExists`, raw predicates, expression joins, nested relations, `GROUP BY`, `DISTINCT`, and calculated columns — requires `dependsOn()` or `dependsOnTables()`.

- **Debug Warning:** If `app.debug` is true, NormCache will log a warning if it detects a query touching a table not declared in `dependsOn()`.

### Cluster Mode & Consistency

- **Single Node / Hash Tagging:** Provides strong multi-key atomicity.
- **Slotting Mode:** Offers better distribution but weaker cross-key atomicity. Multi-dependency queries are automatically routed to the result cache in slotting mode to prevent cross-slot consistency errors.

### Stale Serving

To prevent cache stampedes, NormCache may serve slightly stale data (up to the configured stale depth) while a background writer rebuilds the cache. This guarantees high availability under extreme load at the cost of immediate read-your-writes consistency.

---

## Performance

- **Single round trip on cache hit** — version check + ID fetch + model `MGET` in one Lua `EVAL`.
- **`MGET` for bulk reads** — all model attributes for a result set in one Redis call.
- **No scanning on invalidation** — version bump makes stale keys unreachable; TTL handles eviction. (Manual operations like `flushAll()` and tag flushing do use `SCAN`).
- **Stampede protection** — waiters `BRPOP` a wake channel (200ms) instead of storming the DB. Requires Redis 6.0+ for sub-second precision; both PhpRedis and Predis support this.
- **igbinary support** — smaller payloads and faster serialization when the extension is installed.

---

## License

MIT
