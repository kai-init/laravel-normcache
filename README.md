# Laravel Normcache

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

**Laravel caching, redesigned around normalized models.**

Most cache packages store full query results. Normcache stores IDs and models separately, then reconstructs results at read time — the same way normalized frontend stores (Redux, Apollo) work. This makes invalidation instant and storage efficient, regardless of how many different queries touch the same model.

## The problem with traditional query caching

```
// Traditional: cache the full result set
User::where('active', true)->get()  →  cache full User objects

// When any user changes, you must invalidate this key.
// But you also have:
User::where('role', 'admin')->get()
User::where('country', 'AU')->get()
User::orderBy('created_at')->paginate(20)
// ...and dozens more query shapes, all stale.
```

Tracking which cache keys to invalidate becomes a dependency graph problem. Most packages solve this with tags or scans — both expensive at scale.

## The normcache approach

```
// Layer 1 — query cache (stores only IDs, versioned)
User::where('active', true)->get()
  →  query:v14:a3f9...  =  [1, 5, 9, 22]

// Layer 2 — model cache (stores model attributes by PK)
  →  model:user:1   =  { id: 1, name: "Kai", ... }
  →  model:user:5   =  { id: 5, name: "Alice", ... }
  →  ...
```

The same model entry `model:user:5` is reused across every query that includes user 5. There is no duplication.

## How versioned invalidation works

Each model class has a Redis version counter:

```
ver:user  =  14
```

Version is embedded in every query cache key:

```
query:v14:a3f9...  →  [1, 5, 9]
query:v14:b82c...  →  [1, 22]
```

When any user is written:

```
INCR ver:user   →   15
```

All `v14` keys are now permanently bypassed — no deletes, no scans, no tag lookups. They simply expire naturally. The next read writes fresh `v15` keys.

```
┌─────────────────────────────────────────────────────┐
│  User::where('active', true)->get()                 │
│                                                     │
│  1. Check  query:v15:a3f9...  →  cache miss         │
│  2. SELECT id FROM users WHERE active = 1           │
│     →  [1, 5, 9]                                    │
│  3. MGET model:user:1, model:user:5, model:user:9   │
│     →  hits: [1, 5]   misses: [9]                   │
│  4. SELECT * FROM users WHERE id IN (9)  (miss only)│
│  5. Return hydrated collection                      │
└─────────────────────────────────────────────────────┘
```

Individual model entries are reused across all query shapes. A cache hit on `model:user:5` serves every query that includes user 5, regardless of how the query was structured.

## Requirements

- PHP 8.2+
- Laravel 11+
- Redis (PhpRedis or Predis)

## Installation

```bash
composer require kai-init/laravel-normcache
```

Publish the config:

```bash
php artisan vendor:publish --tag=normcache-config
```

## Setup

Add the `NormCacheable` trait to any Eloquent model you want cached:

```php
use NormCache\Traits\NormCacheable;

class User extends Model
{
    use NormCacheable;
}
```

That's it. All queries on that model now go through the two-layer cache automatically.

## Usage

### Basic queries

```php
// Cached automatically
User::all();
User::where('active', true)->get();
User::find(1);
User::paginate(20);
User::cursorPaginate(20);
```

### Bypassing the cache

```php
User::withoutCache()->get();
```

### Per-query TTL

```php
// Cache this result for 10 minutes regardless of global TTL
User::query()->remember(600)->get();
```

### Aggregates with caching

```php
// withCount, withSum, withAvg, withMin, withMax, withExists
User::cacheAggregates()->withCount('posts')->get();
```

### Manual flush

```bash
# Flush a specific model
php artisan normcache:flush --model="App\Models\User"

# Flush everything
php artisan normcache:flush
```

Or programmatically:

```php
use NormCache\Facades\NormCache;

NormCache::flushModel(User::class);
NormCache::flushAll();
```

## Observability

Normcache fires events on every cache operation with zero overhead when no listeners are registered:

```php
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;

// Wire into Pulse, Telescope, StatsD, or a simple log
Event::listen(QueryCacheMiss::class, function (QueryCacheMiss $e) {
    Log::debug("Query miss: {$e->modelClass}", ['key' => $e->key]);
});

Event::listen(ModelCacheMiss::class, function (ModelCacheMiss $e) {
    Pulse::record('model_cache_miss', $e->modelClass, count($e->ids));
});
```

| Event | Fired when | Properties |
|---|---|---|
| `QueryCacheHit` | Query ID list served from Redis | `modelClass`, `key` |
| `QueryCacheMiss` | ID list not cached — DB queried | `modelClass`, `key` |
| `ModelCacheHit` | Model attributes served from Redis | `modelClass`, `ids[]` |
| `ModelCacheMiss` | Attributes not cached — DB queried | `modelClass`, `ids[]` |

## Configuration

```php
// config/normcache.php
return [
    'connection'  => env('NORMCACHE_CONNECTION', 'cache'),
    'enabled'     => env('NORMCACHE_ENABLED', true),
    'ttl'         => env('NORMCACHE_TTL', 604800),        // model keys: 7 days
    'query_ttl'   => env('NORMCACHE_QUERY_TTL', 3600),    // query keys: 1 hour
    'key_prefix'  => env('NORMCACHE_PREFIX', ''),
    'cooldown'    => env('NORMCACHE_COOLDOWN', 0),        // debounce rapid writes (seconds)
];
```

**`cooldown`** — When set, consecutive writes to the same model within the cooldown window only bump the version once. Useful for write-heavy models where you want to avoid thrashing the version counter.

## What gets cached, what doesn't

Normcache caches queries it can fully reconstruct from a list of primary keys. Queries with joins, GROUP BY, HAVING, UNION, raw ORDER BY, aggregate functions (unless opted-in), or pessimistic locks bypass the cache automatically and hit the database directly — no configuration needed.

## Transaction safety

Invalidations that happen inside a database transaction are deferred until the transaction commits. If the transaction rolls back, the cache is untouched — the version counter is never bumped and no model keys are evicted.

## Performance notes

- **Invalidation is O(1)**: one `INCR` on a version key, regardless of how many cached queries exist for that model.
- **Bulk reads use `MGET`**: all model keys for a result set are fetched in a single Redis round-trip.
- **Writes use pipelining**: cache warm-up for missed model keys is batched in one pipeline call.
- **Bulk deletes use `UNLINK`**: non-blocking async deletion (Redis 4.0+) with 1000-key chunking.
- **No cache scanning on invalidation**: version shift makes stale keys unreachable without touching them.

## License

MIT
