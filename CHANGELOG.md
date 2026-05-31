# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.1] — 2026-05-31

### Added

- **Cooldown invalidation across all cache families:** raw, scalar, pivot, and aggregate Lua scripts now apply due scheduled invalidations before composing version segments, matching `fetch_versioned_query`.
- **`stale_ttl_depth` config:** controls stale-serving depth during stampede protection. Set to `0` to disable. Default: `3`.
- **`EloquentContractTest` suite:** 130+ contract tests verifying that cold cache, warm cache, and native Eloquent return identical results across every intercepted operation — `get`, `first`, `paginate`, all scalar aggregates, all relation shapes, all `withAggregate` variants, collection loading, global scopes, bypass paths, and write shapes.

### Fixed

- **`withAggregate` parameter order:** signature now matches Laravel's `($relations, $column, $function)`. The previous reversal worked for simple columns but produced invalid SQL for complex `DB::raw()` expressions.
- **`withCount` with removed parent global scopes:** `fetchMissed()` now propagates the parent builder's removed scopes to the aggregate sub-query, fixing incorrect counts when `withoutGlobalScope()` was active.
- **`prepareMissedQuery` removed scopes:** the `preserveQueryShape=true` branch now replays removed global scopes on the fallback DB query.
- **`HasOneThrough` + `latestOfMany()` warm-cache:** the through-relation warm path no longer returns `null` when `latestOfMany()` adds synthetic join columns to the projection.

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
