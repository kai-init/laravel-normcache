# Laravel Normcache

**Normalized, self-invalidating Redis caching for Laravel Eloquent.**

[![Tests](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml/badge.svg)](https://github.com/kai-init/laravel-normcache/actions/workflows/tests.yml)
[![PHPStan](https://img.shields.io/badge/PHPStan-level%205-brightgreen.svg)](phpstan.neon)
[![Latest Version on Packagist](https://img.shields.io/packagist/v/kai-init/laravel-normcache.svg)](https://packagist.org/packages/kai-init/laravel-normcache)
[![License](https://img.shields.io/github/license/kai-init/laravel-normcache.svg)](LICENSE)

Normcache caches query results as ID lists and stores model attributes in versioned model keys. When a model changes, Normcache bumps a version key instead of scanning and deleting every query that may have returned that model.

**Requirements:** PHP 8.2+, Laravel 12/13, Redis 6.0+

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

Redis Cluster sharding is now fully atomic within each cache space. Normcache keeps the keys for a cached operation and its valid dependencies in one hash slot, so cache reads, rebuilds, and invalidation coordination remain atomic.

- **Cache spaces:** declare `$normCacheSpaces` on a model and select a declared space with `->space()` when needed.
- **Space-targeted flushing:** use `NormCache::flushAll('space')` or `php artisan normcache:flush --space=...`.
- **Named table dependencies:** `dependsOnTables()` works in named spaces and is invalidated with `invalidateTableVersion()`.

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

`count`, `exists`, `value`, `pluck`, `sum`, `avg`, `min`, `max`, pagination totals, and `withCount` / `withSum` / `withAvg` / `withMin` / `withMax` / `withExists` are cached when their dependencies are safe.

Eager-loaded `BelongsTo`, `BelongsToMany`, `MorphTo`, `MorphToMany`, `MorphedByMany`, `HasManyThrough`, and `HasOneThrough` relations are cached. `attach`, `detach`, `sync`, and `updateExistingPivot` invalidate the relevant pivot cache.

## Invalidation

Eloquent writes on cacheable models invalidate automatically. For manual invalidation:

```php
use NormCache\Facades\NormCache;

NormCache::flushModel(Post::class);
NormCache::flushAll();
NormCache::flushAll('content');
```

```bash
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

Cache spaces are Normcache's Redis Cluster sharding boundary. Each space has a Redis hash tag, so a cached operation stays within one Cluster slot.

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

If a model dependency is not valid in the active space, Normcache bypasses the cache by default. Set `spaces.cross_space_behavior` to `throw` to fail loudly during development. Raw table dependencies from `dependsOnTables()` can be used in any active space and are invalidated with `invalidateTableVersion()`.

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
