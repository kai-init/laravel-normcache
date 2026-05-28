# Laravel Normcache

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

**Memory Efficient, Normalized, self-invalidating Redis cache for Laravel Eloquent.**
**Cluster Ready**

## The Core Idea

Most caching packages cache query results as a single blob — the entire collection, serialized and stored together. Normcache takes a different approach: it stores the list of matching IDs separately from the model data, and keeps each model's attributes in its own key. Every model is stored exactly once, no matter how many queries return it.

```
Query cache  →  query:{posts}:v3:...  →  [4, 7, 12]
Model cache  →  model:{posts}:4      →  { id:4, title:..., body:... }
             →  model:{posts}:7      →  { id:7, title:..., body:... }
             →  model:{posts}:12     →  { id:12, title:..., body:... }
```

When post 7 is updated, Normcache deletes `model:{posts}:7` and increments a version counter. The version is embedded in every query cache key, so all cached queries that returned post 7 — filtered, paginated, or sorted however they were — automatically miss on the next read. No index of which queries to invalidate is needed.

### Why this matters

- **You never store the same record twice.**
  - A popular model appearing in 50 cached query results is stored once, not 50 times. This massively reduces your Redis memory usage.
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

### Cross-table queries (`dependsOn`)

Queries that filter across tables are not cached by default because Normcache cannot automatically determine which model invalidations should make the result stale. `dependsOn()` lets you declare the dependency explicitly:

```php
// Cache a whereHas query whose result depends on Post invalidations:
Author::whereHas('posts', fn($q) => $q->where('published', true))
    ->dependsOn([Post::class])
    ->get();
```

`dependsOn([...])` tells the cache which additional model classes affect this query's result, so invalidating any of those models invalidates this cached entry. It does **not** assert that the query result can be reconstructed from the model cache — queries with `JOIN`, `DISTINCT`, `GROUP BY`, `HAVING`, `UNION`, aggregate functions, calculated columns, or pessimistic locks will still bypass the cache and hit the database directly, regardless of whether `dependsOn()` is set.

```php
// These still bypass the cache even with dependsOn():
Author::join('posts', 'posts.author_id', '=', 'authors.id')->dependsOn([Post::class])->get(); // → DB
Post::select('author_id')->distinct()->dependsOn([Post::class])->get();      // → DB
Post::select(DB::raw('COUNT(*)'))->dependsOn([Post::class])->get();           // → DB
Post::groupBy('author_id')->dependsOn([Post::class])->get();                  // → DB
Post::lockForUpdate()->dependsOn([Post::class])->get();                       // → DB
```

### Per-query TTL

```php
Post::query()->remember(600)->get(); // cache this result for 10 minutes
```

### Aggregates

`withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached automatically. Aggregate values are stored per model ID and invalidated when related rows change or when the relationship membership changes on the parent model.

```php
Post::withCount('comments')->get();
Post::withSum('orders', 'total')->get();
```

To skip aggregate caching for a specific query:

```php
Post::withoutAggregateCache()->withCount('comments')->get();
```

### Relationship caching

`BelongsTo`, `BelongsToMany`, `MorphToMany`, `MorphedByMany`, `HasManyThrough`, and `HasOneThrough` relationships are cached for eligible eager-loads. On a warm hit no SQL is executed. `HasOne`, `HasMany`, `MorphOne`, and `MorphMany` are cached via the query cache when the related model uses `Cacheable`.

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

If you mutate cacheable model tables outside Eloquent, flush the affected model cache manually after the committed write. Normcache only observes writes that go through cacheable Eloquent models/builders; raw SQL and `DB::table(...)` calls bypass automatic invalidation.

```php
DB::table('posts')->where('published', false)->update(['published' => true]);

NormCache::flushModel(Post::class);
```

Runtime connection switching on the same cacheable model class, such as `Post::on('replica')`, is not supported. Cache namespaces are derived from the model class and its declared/default connection, so use distinct model classes with fixed `$connection` values if you need isolated caches per database connection.

---

## Redis Cluster

Normcache is optimised for Redis Cluster. Every key uses a hash tag derived from the model table name — `{posts}`, `{users}`, etc. If a model declares a connection name, that connection is included too, for example `{analytics:posts}`. This keeps fixed model classes on different database connections isolated while still placing all keys for a given model on the same cluster slot. This means:

- `MGET` batches across an entire result set are always single-slot and never cross node boundaries.
- Lua scripts (`EVAL`) that combine a version read + data fetch in one round trip are always operating on co-located keys.
- Pipelines that write model attributes and register them in a member set never split across nodes.

Enable cluster mode in the config:

```php
// config/normcache.php
'cluster' => env('NORMCACHE_CLUSTER', false),
```

`flushAll()` is supported in cluster mode.

---

## What gets cached

Normcache caches Eloquent reads that can be reduced to:

- a versioned list of model IDs
- per-model attribute payloads keyed by model ID

That includes:

- Normal cacheable-model queries such as `all()`, `where(...)`, `first()`, `find()`, and `paginate()`
- Primary-key lookups such as `whereKey($id)`, `where('id', $id)`, and eligible `whereIn('id', [...])` queries
- Aggregates via `withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists`
- Eager-loaded relationships. `MorphTo` uses the model cache per morph type (falls back to DB for non-`Cacheable` related types, soft-delete scopes, or per-type constraints). `MorphedByMany` is fully supported via `CacheableMorphToMany`.
- Simple column projections and aliases when the result can still be rebuilt from cached model attributes

## What bypasses the cache

The following query types always hit the database directly:

| Query feature                                        | Reason                                                      |
| ---------------------------------------------------- | ----------------------------------------------------------- |
| `JOIN`                                               | Result depends on joined table, not just this model         |
| `GROUP BY` / `HAVING`                                | Aggregated results can't be mapped to individual model keys |
| `UNION`                                              | Multi-model result set                                      |
| `DISTINCT`                                           | Result shape is not treated as a normalized model query     |
| Raw `ORDER BY`                                       | Can't be applied to cached key list                         |
| `SELECT` with expressions or computed columns        | Computed values are not stored in the model cache           |
| Subquery where clauses (`EXISTS`, `IN (subquery)`)   | Query shape is not treated as a pure model query            |
| Pessimistic locking (`lockForUpdate` / `sharedLock`) | Must always read from DB                                    |
| Inside a database transaction                        | Reads inside a transaction must see uncommitted data        |
| Raw SQL / `DB::table(...)`                           | Bypasses cacheable Eloquent models and builders             |

Two important nuances:

- A query cache hit means the cached ID list was reused. Some model keys may still be missing and get repaired from the database on demand.
- Normcache caches model-backed result sets, not arbitrary SQL shapes. If the result cannot be rebuilt from normalized model attributes, it bypasses the cache.

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

| Event            | Fired when                         | Properties            |
| ---------------- | ---------------------------------- | --------------------- |
| `QueryCacheHit`  | ID list served from Redis          | `modelClass`, `key`   |
| `QueryCacheMiss` | ID list not cached — DB queried    | `modelClass`, `key`   |
| `ModelCacheHit`  | Model attributes served from Redis | `modelClass`, `ids[]` |
| `ModelCacheMiss` | Attributes not cached — DB queried | `modelClass`, `ids[]` |

---

## Configuration

```php
// config/normcache.php
return [
    'connection'     => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled'        => env('NORMCACHE_ENABLED', true),
    'ttl'            => env('NORMCACHE_TTL', 604800),      // model keys: 7 days
    'query_ttl'      => env('NORMCACHE_QUERY_TTL', 3600),  // query/pivot/through keys: 1 hour
    'key_prefix'     => env('NORMCACHE_PREFIX', ''),
    'cooldown'       => env('NORMCACHE_COOLDOWN', 0),      // version bump debounce in seconds
    'cluster'        => env('NORMCACHE_CLUSTER', false),
    'events'         => env('NORMCACHE_EVENTS', true),     // fire cache hit/miss events
    'fallback'       => env('NORMCACHE_FALLBACK', false),  // fall back to DB on Redis error
    'fire_retrieved' => env('NORMCACHE_FIRE_RETRIEVED', false),
];
```

**`cooldown`** — Consecutive writes within the cooldown window only bump the version once. Useful for write-heavy models to avoid thrashing the version counter.

**`events`** — Set to `false` to disable all `QueryCacheHit`, `QueryCacheMiss`, `ModelCacheHit`, and `ModelCacheMiss` event dispatches. For production hot paths, prefer `NORMCACHE_EVENTS=false` unless you actively consume these events for observability.

**`fallback`** — When `true`, any Redis exception during a read is caught, reported via `report()`, the cache is disabled for the remainder of the request, and the query falls back to the database. When `false` (the default), Redis errors propagate normally. Enable this if you want your application to stay available during Redis outages.

**`fire_retrieved`** — When `true`, models hydrated from Redis fire Eloquent's `retrieved` event. It is disabled by default to avoid event overhead on cache hits.

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
