# Laravel Normcache

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

Most caching packages cache query results as a single blob — the entire collection, serialized and stored together. Normcache takes a different approach: it stores the list of matching IDs separately from the model data, and keeps each model's attributes in its own key.Every model is stored once across all queries — a single version bump invalidates everything that returned it, in O(1).

```
query:{posts}:v3:...  →  [4, 7, 12]
model:{posts}:4       →  { id:4, title:..., body:... }
model:{posts}:7       →  { id:7, title:..., body:... }
model:{posts}:12      →  { id:12, title:..., body:... }
```

**Requirements:** PHP 8.2+, Laravel 11/12/13, Redis 4.0+

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

Queries that span multiple tables are not cached by default — Normcache can't infer which model writes should invalidate them. `dependsOn()` lets you declare the dependency explicitly:

```php
Author::whereHas('posts', fn($q) => $q->where('published', true))
    ->dependsOn([Post::class])
    ->get();

// Works for any query shape — JOIN, GROUP BY, DISTINCT, subquery WHERE, raw ORDER BY:
Author::join('posts', 'posts.author_id', '=', 'authors.id')->dependsOn([Post::class])->get();
Post::select('author_id', DB::raw('SUM(views) as total'))->groupBy('author_id')->dependsOn([Post::class])->get();
```

All `dependsOn` queries are cached as versioned raw rows. When any declared model class is written, the versioned key becomes unreachable and the next read re-populates from the database. Pessimistic locks always bypass the cache.

**List every table the query reads.** An under-declared dependency means silent staleness until TTL — there is no backstop.

### Per-query TTL

```php
Post::query()->remember(600)->get();
```

### Aggregates

`withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached automatically and invalidated when related rows change.

```php
Post::withCount('comments')->get();
Post::withoutAggregateCache()->withCount('comments')->get(); // skip aggregate cache
```

### Relationship caching

`BelongsTo`, `BelongsToMany`, `MorphToMany`, `MorphedByMany`, `HasManyThrough`, and `HasOneThrough` are cached for eager loads — on a warm hit no SQL is executed. `HasOne`, `HasMany`, `MorphOne`, and `MorphMany` are cached via the query cache when the related model uses `Cacheable`.

`attach`, `detach`, `sync`, and `updateExistingPivot` automatically invalidate the relevant pivot cache.

### Manual flush

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

#### Tag-based flush

Tag any query to group cache entries for manual flushing — useful for invalidation events the version system can't see (deploys, config changes, nightly rebuilds):

```php
Author::whereHas('posts')->dependsOn([Post::class])->tag('homepage')->get();

NormCache::flushTag(Author::class, 'homepage');   // single model — single-slot scan
NormCache::flushTagAcrossModels('homepage');       // all models — cluster-wide scan
```

> `Post::on('replica')` is not supported. Use distinct model classes with a fixed `$connection` for per-connection cache isolation.

---

## What bypasses the cache

| Query feature                                        | Workaround            |
| ---------------------------------------------------- | --------------------- |
| Pessimistic locking (`lockForUpdate` / `sharedLock`) | None — must hit DB    |
| Inside a database transaction                        | None — must hit DB    |
| Raw SQL / `DB::table(...)`                           | None — flush manually |

Everything else — `JOIN`, `GROUP BY`, `DISTINCT`, subquery `WHERE`, raw `ORDER BY`, calculated columns — is cacheable with `dependsOn()`.

---

## Configuration

```php
// config/normcache.php
return [
    'connection'     => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled'        => env('NORMCACHE_ENABLED', true),
    'ttl'            => env('NORMCACHE_TTL', 604800),      // model keys: 7 days
    'query_ttl'      => env('NORMCACHE_QUERY_TTL', 3600),  // query/raw/pivot keys: 1 hour
    'key_prefix'     => env('NORMCACHE_PREFIX', ''),
    'cooldown'       => env('NORMCACHE_COOLDOWN', 0),      // version bump debounce in seconds
    'cluster'        => env('NORMCACHE_CLUSTER', false),
    'events'         => env('NORMCACHE_EVENTS', true),
    'fallback'       => env('NORMCACHE_FALLBACK', false),
    'fire_retrieved' => env('NORMCACHE_FIRE_RETRIEVED', false),
    'debugbar'       => env('NORMCACHE_DEBUGBAR', false),
];
```

**`cooldown`** — Consecutive writes within the window bump the version only once. Useful for write-heavy models.

**`fallback`** — When `true`, Redis exceptions are caught, the cache is disabled for the request, and queries fall back to the database.

**`events`** — Set to `false` to disable hit/miss event dispatches on hot paths.

**`fire_retrieved`** — When `true`, models hydrated from Redis fire Eloquent's `retrieved` event (disabled by default).

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

## Redis Cluster

Every key uses a hash tag derived from the model class — `{posts}`, `{analytics:posts}` — so all keys for a given model land on the same cluster slot. `MGET` batches, Lua scripts, and pipelines never cross node boundaries.

Enable with `'cluster' => true` in the config. `flushAll()` is supported.

---

## Performance

- **Single round trip on cache hit** — version check + ID fetch + model `MGET` in one Lua `EVAL`.
- **`MGET` for bulk reads** — all model attributes for a result set in one Redis call.
- **No scanning on invalidation** — version bump makes stale keys unreachable; TTL handles eviction.
- **igbinary support** — smaller payloads and faster serialization when the extension is installed.

---

## License

MIT
