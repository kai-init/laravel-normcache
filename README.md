# Laravel Normcache

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

**Memory Efficient, Normalized, self-invalidating Redis cache for Laravel Eloquent.**
**Cluster Ready**

## The Core Idea

Most caching packages cache query results as a single blob — the entire collection, serialized and stored together. Normcache takes a different approach: it stores the list of matching IDs separately from the model data, and keeps each model's attributes in its own key. Every model is stored exactly once, no matter how many queries return it.

```
Query cache  →  "posts where active=1, page 2"  →  [4, 7, 12]
Model cache  →  post:4  →  { id:4, title:..., body:... }
             →  post:7  →  { id:7, title:..., body:... }
             →  post:12 →  { id:12, title:..., body:... }
```

When post 7 is updated, Normcache deletes `post:7` and increments a version counter. The version is embedded in every query cache key, so all cached queries that returned post 7 — filtered, paginated, or sorted however they were — automatically miss on the next read. No index of which queries to invalidate is needed.

### Why this matters

- **You never store the same record twice.**
  - A popular model appearing in 50 cached query results is stored once, not 50 times. This massively reduces your redis memory usage.
- **Warming one query warms every query.**
  - When a model is fetched by ID, its attributes land in the model cache. Every other query that later includes that model — a search, a paginated list, a relationship — gets a model cache hit on that record for free.
- **Invalidation is O(1).**
  - A single `INCR` on a version key makes all cached queries for a model stale. No tag scanning, no key enumeration, no O(n) overhead as your cache grows.

---

**Requirements:**
- PHP 8.2+
- Laravel 11, 12, or 13
- Redis 4.0+

---

## Installation

```bash
composer require kai-init/laravel-normcache
```

Publish the config:

```bash
php artisan vendor:publish --tag=normcache-config
```

---

## Setup

Add the `Cacheable` trait to any Eloquent model you want cached:

```php
use NormCache\Traits\Cacheable;

class Post extends Model
{
    use Cacheable;
}
```

That's it. All queries on that model now go through the two-layer cache automatically.

---

## Usage

### Basic queries

```php
Post::all();
Post::where('active', true)->get();
Post::find(1);
Post::paginate(20);
```

### Bypassing the cache

```php
Post::withoutCache()->get();
```

### Per-query TTL

```php
Post::query()->remember(600)->get(); // cache this result for 10 minutes
```

### Aggregates

`withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached automatically. Aggregate values are stored per model ID and invalidated when the related model changes.

```php
Post::withCount('comments')->get();
Post::withSum('orders', 'total')->get();
```

To skip aggregate caching for a specific query:

```php
Post::withoutAggregateCache()->withCount('comments')->get();
```

### Relationship caching

`BelongsToMany`, `MorphToMany`, `HasManyThrough`, and `HasOneThrough` relationships are cached when eager-loaded. On a warm hit no SQL is executed.

```php
// First load: runs SQL, caches pivot map + related models
Post::with('tags')->get();

// Subsequent loads: zero SQL
Post::with('tags')->get();
```

`attach`, `detach`, `sync`, and `updateExistingPivot` automatically invalidate the relevant pivot cache.

### Manual flush

```bash
php artisan normcache:flush --model="App\Models\Post"
php artisan normcache:flush   # flush everything
```

```php
use NormCache\Facades\NormCache;

NormCache::flushModel(Post::class);
NormCache::flushAll();
```

---

## Redis Cluster

Normcache is optimised for Redis Cluster. Every key uses a hash tag derived from the model class name — `{post}`, `{user}`, etc. — so all keys for a given model land on the same cluster slot. This means:

- `MGET` batches across an entire result set are always single-slot and never cross node boundaries.
- Lua scripts (`EVAL`) that combine a version read + data fetch in one round trip are always operating on co-located keys.
- Pipelines that write model attributes and register them in a member set never split across nodes.

Enable cluster mode in the config:

```php
// config/normcache.php
'cluster' => env('NORMCACHE_CLUSTER', false),
```

> **Note:** `flushAll()` is not supported in cluster mode. To perform a full flush on a cluster, use `NormCache::getFlushPatterns()` to get the key patterns and run your own per-node scan and delete:
>
> ```php
> $patterns = NormCache::getFlushPatterns();
> // ['query:*', 'model:*', 'ver:*', ...]
>
> // Scan and UNLINK each pattern on every master node using your preferred approach.
> ```

---

## What bypasses the cache

The following query types always hit the database directly:

| Query feature | Reason |
|---|---|
| `JOIN` | Result depends on joined table, not just this model |
| `GROUP BY` / `HAVING` | Aggregated results can't be mapped to individual model keys |
| `UNION` | Multi-model result set |
| Raw `ORDER BY` | Can't be applied to cached key list |
| `SELECT` with expressions | Computed columns aren't in the model cache |
| Pessimistic locking (`lockForUpdate` / `sharedLock`) | Must always read from DB |
| Inside a database transaction | Reads inside a transaction must see uncommitted data |

---

## Transaction safety

Invalidations inside a database transaction are deferred until commit. On rollback, nothing is touched — the version counter is never bumped and no model keys are evicted.

---

## Observability

```php
use NormCache\Events\{QueryCacheHit, QueryCacheMiss, ModelCacheHit, ModelCacheMiss};

Event::listen(QueryCacheMiss::class, fn($e) => Pulse::record('query_miss', $e->modelClass));
Event::listen(ModelCacheMiss::class, fn($e) => Pulse::record('model_miss', $e->modelClass, count($e->ids)));
```

| Event | Fired when | Properties |
|---|---|---|
| `QueryCacheHit` | ID list served from Redis | `modelClass`, `key` |
| `QueryCacheMiss` | ID list not cached — DB queried | `modelClass`, `key` |
| `ModelCacheHit` | Model attributes served from Redis | `modelClass`, `ids[]` |
| `ModelCacheMiss` | Attributes not cached — DB queried | `modelClass`, `ids[]` |

---

## Configuration

```php
// config/normcache.php
return [
    'connection'  => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled'     => env('NORMCACHE_ENABLED', true),
    'ttl'         => env('NORMCACHE_TTL', 604800),      // model keys: 7 days
    'query_ttl'   => env('NORMCACHE_QUERY_TTL', 3600),  // query/pivot/through keys: 1 hour
    'key_prefix'  => env('NORMCACHE_PREFIX', ''),
    'cooldown'    => env('NORMCACHE_COOLDOWN', 0),      // version bump debounce in seconds
    'cluster'     => env('NORMCACHE_CLUSTER', false),
    'events'      => env('NORMCACHE_EVENTS', true),     // fire cache hit/miss events
    'fallback'    => env('NORMCACHE_FALLBACK', false),  // fall back to DB on Redis error
];
```

**`cooldown`** — Consecutive writes within the cooldown window only bump the version once. Useful for write-heavy models to avoid thrashing the version counter.

**`events`** — Set to `false` to disable all `QueryCacheHit`, `QueryCacheMiss`, `ModelCacheHit`, and `ModelCacheMiss` event dispatches. Useful in high-throughput scenarios where the event overhead is not needed.

**`fallback`** — When `true`, any Redis exception during a read is caught, reported via `report()`, the cache is disabled for the remainder of the request, and the query falls back to the database. When `false` (the default), Redis errors propagate normally. Enable this if you want your application to stay available during Redis outages.

---

## Performance

- **Single round trip on cache hit** — version read + query ID fetch + model MGET are combined into one Lua `EVAL` call.
- **Cached paginate count** — `paginate()` caches the `COUNT(*)` query under a versioned key so navigating between pages never re-runs the count query.
- **Invalidation is O(1)** — one `INCR` on a version key, regardless of how many cached queries exist for that model.
- **`MGET` for bulk reads** — all model attributes for a result set in one Redis call.
- **Pipelined writes** — cache warm-up for missed models is batched in a single pipeline.
- **`UNLINK` for deletes** — non-blocking async deletion (Redis 4.0+), chunked at 1000 keys.
- **No scanning on invalidation** — version shift makes stale keys unreachable without touching them. Eviction is handled by TTL.
- **igbinary support** — when the `igbinary` PHP extension is installed, model attributes are serialized with igbinary for faster serialization and smaller payloads.
- **In-process version cache** — version numbers are cached in-process per request (with Octane support) to eliminate redundant Redis reads within the same request.

---

## License

MIT
