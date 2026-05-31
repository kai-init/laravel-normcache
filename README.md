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

**Requirements:** PHP 8.2+, Laravel 11/12/13, Redis 4.0+

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

Queries that span multiple tables are not cached by default — Normcache can't infer which model writes should invalidate them. `dependsOn()` lets you declare the dependency explicitly:

```php
Author::whereHas('posts', fn($q) => $q->where('published', true))
    ->dependsOn([Post::class])
    ->get();

// Works for any query shape — JOIN, GROUP BY, DISTINCT, subquery WHERE, raw ORDER BY:
Author::join('posts', 'posts.author_id', '=', 'authors.id')->dependsOn([Post::class])->get();
Post::select('author_id', DB::raw('SUM(views) as total'))
    ->groupBy('author_id')->dependsOn([Post::class])->get();
```

All `dependsOn` queries are cached as versioned raw rows. When any declared model class is written, the versioned key becomes unreachable and the next read re-populates from the database. Pessimistic locks always bypass the cache.

**List every table the query reads.** An under-declared dependency means silent staleness until TTL. Use `tag()` / `flushTag()` when you need manual invalidation for events the model version system cannot see.

### Per-Query TTL

```php
Post::query()->remember(600)->get();
```

### Aggregates

`withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached automatically and invalidated when related rows change.

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

| Query feature                                        | Workaround            |
| ---------------------------------------------------- | --------------------- |
| Pessimistic locking (`lockForUpdate` / `sharedLock`) | None — must hit DB    |
| Inside a database transaction                        | None — must hit DB    |
| Raw SQL / `DB::table(...)`                           | None — flush manually |

Everything else — `JOIN`, `GROUP BY`, `DISTINCT`, subquery `WHERE`, raw `ORDER BY`, calculated columns — is cacheable with `dependsOn()`.

---

## Limitations

- Normcache only hooks Eloquent models that use the `Cacheable` trait. Query builder calls such as `DB::table(...)`, `DB::select()`, and `DB::statement()` are never cached.
- Writes outside Eloquent are invisible to the model version system. Flush the affected model or tag manually after imports, raw updates, maintenance jobs, or external syncs.
- Dynamic connection switching (`Post::on('replica')`) is not supported. Use separate model classes with fixed `$connection` values when the same table is read through multiple connections.
- `dependsOn()` is explicit by design. If a query reads another table, include that model class or manually flush a tag that covers the query.
- Models are expected to use standard single-column primary keys.
- Packages that replace Eloquent builders, relation classes, or hydration behavior may bypass parts of Normcache.

---

## Configuration

```php
// config/normcache.php
return [
    'connection'        => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled'           => env('NORMCACHE_ENABLED', true),
    'ttl'               => env('NORMCACHE_TTL', 604800),
    'query_ttl'         => env('NORMCACHE_QUERY_TTL', 3600),
    'key_prefix'        => env('NORMCACHE_PREFIX', ''),
    'cooldown'          => env('NORMCACHE_COOLDOWN', 0),
    'building_lock_ttl' => env('NORMCACHE_BUILDING_LOCK_TTL', 5),
    'stampede_wait_ms'  => env('NORMCACHE_STAMPEDE_WAIT_MS', 200),
    'stale_version_depth' => env('NORMCACHE_STALE_VERSION_DEPTH', 3),
    'cluster'           => env('NORMCACHE_CLUSTER', false),
    'events'            => env('NORMCACHE_EVENTS', true),
    'fallback'          => env('NORMCACHE_FALLBACK', false),
    'fire_retrieved'    => env('NORMCACHE_FIRE_RETRIEVED', false),
    'debugbar'          => env('NORMCACHE_DEBUGBAR', false),
];
```

- **`ttl`** — Lifetime of individual model attribute keys. Default: 7 days.
- **`query_ttl`** — Lifetime of query, raw, pivot, and through cache keys. Default: 1 hour.
- **`cooldown`** — Useful for write-heavy models. Version bump debounce in seconds. Consecutive writes within the window bump the version only once.
- **`building_lock_ttl`** — How long a cache-build lock is held before it expires and another request can take over.
- **`stampede_wait_ms`** — How long a waiter blocks on a wake channel before falling back to the database. Requires Redis 6.0+ for sub-second precision.
- **`stale_version_depth`** — How many old query-cache versions to serve as stale data while a rebuild is in progress. Set to `0` to disable stale serving. (`NORMCACHE_STALE_TTL_DEPTH` is accepted as a deprecated fallback.)
- **`fallback`** — When `true`, Redis exceptions disable the cache for the request and queries fall back to the database silently.
- **`events`** — Set to `false` to skip hit/miss event dispatches on hot paths.
- **`fire_retrieved`** — When `true`, models hydrated from Redis fire Eloquent's `retrieved` event.

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

Every key uses a hash tag derived from the model class — `{posts}`, `{analytics:posts}` — so all keys for a given model land on the same cluster slot. `MGET` batches, Lua scripts, and pipelines never cross node boundaries.

Enable with `'cluster' => true` in the config. `flushAll()` is supported.

---

## Octane & Horizon

Works out of the box. State is reset between Octane requests and queue jobs — including re-enabling the cache if a Redis error disabled it mid-job.

---

## Performance

- **Single round trip on cache hit** — version check + ID fetch + model `MGET` in one Lua `EVAL`.
- **`MGET` for bulk reads** — all model attributes for a result set in one Redis call.
- **No scanning on invalidation** — version bump makes stale keys unreachable; TTL handles eviction.
- **Stampede protection** — waiters `BRPOP` a wake channel (200ms) instead of storming the DB. Requires Redis 6.0+ for sub-second precision.
- **igbinary support** — smaller payloads and faster serialization when the extension is installed.

---

## License

MIT
