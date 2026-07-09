# Laravel Normcache

**Normalized, self-invalidating Redis caching for Laravel Eloquent.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

Normcache caches query results as ID lists and stores model attributes in versioned model keys. When a model changes, Normcache bumps a version key instead of scanning and deleting every query that may have returned that model.

**Requirements:** PHP 8.2+, Laravel 12/13, Redis 4.0+

## Table of Contents

- [Installation](#installation)
- [What's new in 3.0](#whats-new-in-30)
- [Usage](#usage)
- [Invalidation](#invalidation)
- [Cache spaces](#cache-spaces)
- [Configuration](#configuration)
- [Bypasses and limitations](#bypasses-and-limitations)
- [Observability](#observability)
- [License](#license)

## Installation

```bash
composer require kai-init/laravel-normcache
```

Add `Cacheable` to models you want Normcache to manage:

```php
use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

class Post extends Model
{
    use Cacheable;
}
```

## What's new in 3.0

Normcache 3.0 is a Redis Cluster-oriented release.

- Cache spaces replace the old slotting/sharding configuration.
- Space-targeted flushes are available through `flushAll('space')` and `normcache:flush --space=...`.
- Raw table dependencies from `dependsOnTables()` work in named spaces.
- Model payload invalidation now relies on versioned keys instead of members-set scans.
- `stale_version_depth` remains removed; rebuild coordination uses build locks and wake tokens.

## Usage

Normal Eloquent reads are cached automatically for cacheable models:

```php
Post::all();
Post::where('active', true)->get();
Post::find(1);
Post::paginate(20);
```

Use `withoutCache()` or `ttl()` per query:

```php
Post::withoutCache()->get();
Post::where('active', true)->ttl(600)->get();
```

### Cross-table queries

Simple `whereHas` / `whereDoesntHave` constraints on cacheable relations and plain string joins with an explicit root-table projection are inferred automatically:

```php
Author::whereHas('posts', fn($q) => $q->where('published', true))->get();

Author::join('posts', 'posts.author_id', '=', 'authors.id')
    ->select('authors.*')
    ->get();
```

For other cross-table reads, declare dependencies explicitly:

```php
Author::query()
    ->dependsOn([Post::class])
    ->get();

Author::join('legacy_stats', 'legacy_stats.author_id', '=', 'authors.id')
    ->select('authors.*')
    ->dependsOnTables(['legacy_stats'])
    ->get();
```

`dependsOnTables()` declares a read dependency only. If that table is changed outside Eloquent, call `NormCache::invalidateTableVersion($connection, $table)` after the write.

### Aggregates and relationships

`count`, `exists`, `sum`, `avg`, `min`, `max`, pagination totals, and `withCount` / `withSum` / `withAvg` / `withMin` / `withMax` / `withExists` are cached when their dependencies are safe.

Eager-loaded `BelongsTo`, `BelongsToMany`, `MorphTo`, `MorphToMany`, `MorphedByMany`, `HasManyThrough`, and `HasOneThrough` relations are cached. `attach`, `detach`, `sync`, and `updateExistingPivot` invalidate the relevant pivot cache.

## Invalidation

Eloquent writes on cacheable models invalidate automatically. For manual invalidation:

```php
use NormCache\Facades\NormCache;

NormCache::flushModel(Post::class);
NormCache::flushAll();
NormCache::flushAll('content');

php artisan normcache:flush --model="App\Models\Post"
php artisan normcache:flush
php artisan normcache:flush --space=content
```

If you mutate cacheable tables outside Eloquent, flush the affected model or table version yourself:

```php
DB::table('posts')->update(['published' => true]);
NormCache::flushModel(Post::class);
```

Tags can group query entries for manual flushing:

```php
Author::whereHas('posts')
    ->dependsOn([Post::class])
    ->tag('homepage')
    ->get();

NormCache::flushTag(Author::class, 'homepage');
NormCache::flushTagAcrossModels('homepage');
```

## Cache spaces

Cache spaces are Normcache's Redis Cluster sharding boundary. Each space maps to one Redis hash tag, and every cached plan runs its Lua operations inside the active space.

Models without a declaration use the default space (`{nc}`). Declare named spaces with `$normCacheSpaces`:

```php
use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

class Post extends Model
{
    use Cacheable;

    protected static array $normCacheSpaces = ['content'];
}
```

How space resolution works:

- If no `space()` is selected, a model uses its first declared space as its home space.
- `Post::query()->space('content')` explicitly selects a space.
- `space()` must select a space declared by the model, otherwise Normcache throws an `InvalidArgumentException`.
- A model may declare multiple spaces, up to `spaces.max_per_model`.
- Writes bump the model version in every declared space.

Dependencies must belong to the active space:

```php
class Author extends Model
{
    use Cacheable;

    protected static array $normCacheSpaces = ['content'];
}

Post::query()
    ->space('content')
    ->dependsOn([Author::class])
    ->get();
```

If a model dependency is not valid in the active space, Normcache bypasses the cache by default. Set `spaces.cross_space_behavior` to `throw` to fail loudly during development. Raw table dependencies from `dependsOnTables()` are registered in the active space and invalidated with that space's table version.

Configure placement when you need to control Redis Cluster hash tags:

```php
'spaces' => [
    'max_per_model' => 16,
    'cross_space_behavior' => env('NORMCACHE_CROSS_SPACE_BEHAVIOR', 'bypass'),
    'placement' => [
        'catalog' => ['hash_tag' => 'nc:catalog'],
    ],
],
```

In Redis Cluster mode, broad flushes use the recorded space registry to scan each known space. Standalone Redis uses wildcard hash-tag scans.

## Configuration

Publish `config/normcache.php` if you need to customize runtime behavior:

```bash
php artisan vendor:publish --tag=normcache-config
```

Common options:

| Option                 | Purpose                                                         |
| ---------------------- | --------------------------------------------------------------- |
| `connection`           | Redis connection name. Default: `cache`.                        |
| `enabled`              | Master on/off switch.                                           |
| `ttl`                  | Model attribute key lifetime.                                   |
| `query_ttl`            | Query/result/pivot/through key lifetime.                        |
| `key_prefix`           | Prefix for all Normcache Redis keys.                            |
| `cooldown`             | Debounce version bumps for write-heavy models.                  |
| `building_lock_ttl`    | Cache rebuild lock lifetime.                                    |
| `stampede_wait_ms`     | How long waiters block for a rebuild wake signal.               |
| `stampede_wake_tokens` | Number of waiters to wake after a rebuild.                      |
| `fallback`             | Fail open to the database on Redis errors when `true`.          |
| `events`               | Dispatch cache hit/miss events when `true`.                     |
| `fire_retrieved`       | Fire Eloquent `retrieved` for cached models when `true`.        |
| `debugbar`             | Enable Laravel Debugbar integration when installed.             |
| `spaces.*`             | Cache-space limits, cross-space policy, and hash-tag placement. |

## Bypasses and limitations

Normcache bypasses caching for unsafe reads rather than risking stale or incorrect data.

Always bypassed:

- pessimistic locks (`lockForUpdate`, `sharedLock`)
- reads inside a database transaction
- `DB::table(...)`, `DB::select()`, and raw SQL
- `chunk()`, `each()`, `lazy()`, and `sole()`

Usually require `dependsOn()` or `dependsOnTables()`:

- manual `whereExists`
- raw predicates
- nested relation constraints
- expression joins
- `GROUP BY`, `DISTINCT`, and calculated columns

Other limitations:

- Models should use standard single-column primary keys.
- Writes outside Eloquent are invisible unless you manually flush or invalidate.
- Packages that replace Eloquent builders, relation classes, or hydration behavior may bypass parts of Normcache.
- Normcache caches model connection/table metadata. Call `CacheKeyBuilder::reset()` after switching tenants dynamically.

## Observability

When events are enabled, Normcache dispatches query/model hit and miss events. When `fruitcake/laravel-debugbar` is installed and `normcache.debugbar` is enabled, cache hits, misses, bypasses, and model fetches appear in Debugbar.

## License

MIT
