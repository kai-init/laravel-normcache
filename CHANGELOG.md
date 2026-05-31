# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.1.0] — 2026-05-31

### Added

- **Cooldown invalidation across all cache families:** raw, scalar, and pivot scripts now apply scheduled invalidations before reading, matching the query cache behaviour.
- **`stale_version_depth` config:** controls how many stale versions to serve during stampede protection. Default: `3`, set to `0` to disable.

### Changed

- **Redis Cluster support:** cross-model paths (`dependsOn`, pivot, through, `withCount`) resolve each model's version key individually per slot. Single-instance behaviour is unchanged. Enable with `NORMCACHE_CLUSTER=true`.
- **Aggregate caching simplified:** `withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached as a versioned blob per query. Invalidation and API are unchanged; the per-parent-ID key structure and `RelationAggregateLoader` have been removed.

### Fixed

- **Mutable primary keys:** changing a model's PK via `save()` now evicts the old `model:{table}:id` cache key.
- **Raw build lock tag-segmented:** different tagged queries no longer share the same stampede lock.
- **`where`/`whereRaw` on aggregate alias falls back correctly:** these patterns now trigger the native Eloquent path instead of running a broken ID query.
- **`withAggregate` parameter order:** corrected to match Laravel's `($relations, $column, $function)`.
- **Removed global scopes propagated to aggregate queries:** `withoutGlobalScope()` on a parent query is now respected in aggregate sub-queries and miss-reload paths.
- **`flushModel()` bypasses cooldown:** manual flushes always invalidate immediately.
- **Scalar cache skips expression columns:** `sum`, `avg`, `min`, `max`, `value`, and `pluck` fall through to Eloquent for `DB::raw()` arguments.
- **Raw cache waiter takes over orphaned locks:** a waiter that wakes to a dead builder now populates the cache itself.

---

## [2.0.0] — 2026-05-29

### Added

- **`dependsOn(array $modelClasses)`:** cache cross-table queries by declaring which model classes can invalidate them. Normalization and safety checks still apply.
- **Scalar result caching:** `count`, `sum`, `avg`, `min`, and `max` are cached under a versioned key and invalidated with their parent model. Works with `dependsOn()`.
- **Relation aggregate caching:** `withCount` / `withSum` / other Eloquent relation aggregates are cached per parent and invalidated with related model versions. Non-`Cacheable` related models fall through to Eloquent's computed path and are never cached.
- **`MorphTo` eager-load caching:** each morph type is served from the model cache. Falls back per type when constraints, `morphWithCount`, macros, or a non-`Cacheable` related type are present.
- **`Builder::explain()`:** returns a string describing why a query is cached or bypassed.
- **Debugbar integration:** hits, misses, and bypasses appear on the Debugbar timeline. Absent when Debugbar is not installed.
- **Manual invalidation:** query grouping with `tag()`, `flushTag()`, and `flushTagAcrossModels()`. Tag keys are embedded in `raw`, `query`, `count`, and `scalar` namespaces and flushed atomically.
- **Stampede protection:** cache waiters `BRPOP` a wake channel instead of storming the database. The waiter timeout and build-lock TTL are configurable. Requires Redis 6.0+ for sub-second precision.
- **Queue worker cache recovery:** cache is automatically re-enabled between jobs via `JobProcessed` and `Looping` hooks. Octane requests and tasks reset the same way via `RequestReceived` / `TaskReceived`.

### Fixed

- **CAS-protected writes:** prevent stale query/model data after concurrent invalidation.
- **Redis flush paths:** `flushAll()` and model flushes now use `SCAN` / `SSCAN` instead of loading full key/member sets.
- **Cache correctness:** fixed transaction, Octane, pivot, through, stale-hit, create/flush, and mixed igbinary/PHP behavior.
- **Event and Debugbar instrumentation:** cache hit/miss events and Debugbar coverage now include raw, relation, aggregate, and timeout paths.

### Changed

- **Invalidation coordination:** distributed write lock removed; invalidation is version/CAS based.

---

## [1.1.0] — 2026-05-20

### Added

- **Query hit fast path:** cached ID lists skip a round-trip when all model attributes are in cache.

### Changed

- **Lua scripts:** overhauled for correctness and cluster compatibility.
- **Cache keys:** connection-aware keys prevent cross-connection collisions.

### Fixed

- **Through relations:** fixed cache invalidation gaps for `through`-relation keys.
- **Transactions:** invalidations inside a transaction are flushed atomically on commit.
- **Model reloads:** `fresh()` / `refresh()` now bypasses cache, matching Laravel semantics.
- **Laravel compatibility:** Laravel 11 / 12 fixes.
- **Pivot and through relations:** cache accuracy improvements.

---

## [1.0.2] — 2026-05-14

### Added

- **BelongsTo eager loads:** `CacheableBelongsTo` warms `belongsTo` eager loads from the model cache.
- **Primary-key fast paths:** optimized `whereInRaw`, `limit(0)`, and single-PK lookups.
- **Retrieved events:** `NORMCACHE_FIRE_RETRIEVED` opts in to firing Eloquent `retrieved` on cache hits.

### Changed

- **Model hydration:** rewritten for lower overhead.

---

## [1.0.1] — 2026-05-13

### Fixed

- **Invalidation cooldown:** dropped invalidations are now lazily re-applied.
- **Pivot constraint keys:** pivot constraints are hashed into the key.
- **Transaction invalidation:** cache invalidations inside a transaction are deferred and applied atomically on commit.

---

## [1.0.0] — 2026-05-08

- **Initial release:** first stable Normcache release.
