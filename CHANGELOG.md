# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.0] — 2026-06-03

### Added

- **Planner-driven caching:** query safety, dependency inference, bypass reasons, and cache-mode selection now flow through explicit planning classes.
- **`dependsOn(array $modelClasses)`:** cache cross-table queries by declaring which model classes can invalidate them. Simple dependency-aware queries stay normalized; complex query shapes use a versioned result cache.
- **Versioned result cache:** complex dependency-aware queries, relation aggregates, scalar values, pagination counts, pivot reads, and through-relation reads share versioned invalidation and stampede handling.
- **Scalar result caching:** `count`, `sum`, `avg`, `min`, and `max` are cached under versioned keys and invalidated with their parent and declared dependency models.
- **Relation aggregate caching:** `withCount`, `withSum`, `withAvg`, `withMin`, `withMax`, and `withExists` are cached as versioned result payloads and invalidated by inferred or explicit dependencies.
- **`MorphTo` eager-load caching:** each morph type is served from the model cache when the relation can be safely cached.
- **`Builder::explain()`:** returns a string describing why a query is cached or bypassed.
- **Debugbar integration:** hits, misses, timeouts, and bypasses appear on the Debugbar timeline when Debugbar is installed.
- **Manual invalidation tags:** `tag()`, `flushTag()`, and `flushTagAcrossModels()` group query entries for manual flushing.
- **Stampede protection:** cache builders coordinate with build locks and wake channels; waiters can serve configured stale versions or take over orphaned builds.
- **`stale_version_depth` config:** controls how many stale versions to serve during stampede protection. Default: `3`, set to `0` to disable.
- **Queue worker and Octane recovery:** cache state is reset between jobs, Octane requests, and Octane tasks.

### Changed

- **`Cacheable` namespace:** import the trait from `NormCache\Cacheable` instead of `NormCache\Traits\Cacheable`.
- **Cache-mode selection:** normalized cache is kept for simple primary-table queries, including simple queries with declared dependencies; result cache is used for dependency-aware complex queries and result-style operations.
- **Redis Cluster support:** cross-model paths (`dependsOn`, pivot, through, `withCount`) resolve each model's version key individually per slot. Single-instance behaviour is unchanged. Enable with `NORMCACHE_CLUSTER=true`.
- **Aggregate caching simplified:** relation aggregates are cached as a versioned blob per query. The per-parent-ID aggregate key structure and old aggregate loader path have been removed.
- **Invalidation coordination:** distributed write locks have been replaced by version and CAS-based coordination.
- **Internal structure:** relationship helpers, cache reporting, Redis scripts, query hashing, result payload projection, and builder invalidation code have been reorganized around the new planner and versioned cache flow.

### Fixed

- **CAS-protected writes:** prevent stale query, result, scalar, pivot, and model data after concurrent invalidation.
- **Cooldown invalidation across all cache families:** scalar, result, and pivot scripts now apply scheduled invalidations before reading, matching the query cache behaviour.
- **Redis flush paths:** `flushAll()` and model flushes use `SCAN` / `SSCAN` instead of loading full key/member sets.
- **Mutable primary keys:** changing a model's PK via `save()` now evicts the old `model:{table}:id` cache key.
- **Raw build lock tag-segmented:** different tagged queries no longer share the same stampede lock.
- **`where`/`whereRaw` on aggregate alias falls back correctly:** these patterns now trigger the native Eloquent path instead of running a broken ID query.
- **`withAggregate` parameter order:** corrected to match Laravel's `($relations, $column, $function)`.
- **Removed global scopes propagated to aggregate queries:** `withoutGlobalScope()` on a parent query is now respected in aggregate sub-queries and miss-reload paths.
- **`flushModel()` bypasses cooldown:** manual flushes always invalidate immediately.
- **Scalar cache skips expression columns:** `sum`, `avg`, `min`, `max`, `value`, and `pluck` fall through to Eloquent for `DB::raw()` arguments.
- **Raw cache waiter takes over orphaned locks:** a waiter that wakes to a dead builder now populates the cache itself.

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
