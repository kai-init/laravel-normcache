# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.0]

### Added

- **`dependsOn(array $modelClasses)`** — cache cross-table queries by declaring which model
  classes can invalidate them. Normalization and safety checks still apply.
- **Scalar result caching** — `count`, `sum`, `avg`, `min`, `max` are cached under a versioned
  key and invalidated with their parent model. Works with `dependsOn()`.
- **`MorphTo` eager-load caching** — each morph type is served from the model cache. Falls back
  per-type when constraints, `morphWithCount`, macros (e.g. `withTrashed`), or a non-`Cacheable`
  related type are present.
- **`Builder::explain()`** — returns a string describing why a query is cached or bypassed.
- **Debugbar integration** — hits, misses, and bypasses appear on the Debugbar timeline.
  Absent when Debugbar is not installed.

### Fixed

- **`flushAll()` blocked Redis** with `KEYS` on non-cluster connections. Now uses `SCAN`.
- **`forceFlushModel()` loaded the entire member set into memory.** Now uses `SSCAN` in batches
  of 1 000.
- **Stale model-cache entries could re-enter after a concurrent flush.** Flush paths bump the
  version before deleting keys; writes captured under the old version are rejected via a Lua CAS.
- **Query-cache writes could race a concurrent invalidation.** A Lua CAS verifies all version
  keys before writing. The building lock is always released.
- **Octane: deferred invalidations leaked across requests.** The pending queue is now discarded
  at the start of each request.
- **igbinary / PHP mismatch on mixed deployments.** Format is now detected by the first byte of
  the blob; workers without igbinary get a cache miss instead of a corrupt result.
- Hit events were not fired on stale-key hits.
- Broken `flushModel()` public API introduced in v1.1.0.
- Nested eager loads on pivot cache hits failed to hydrate.
- New-model cache flush left stale entries after `create()`.

### Changed

- **Building-lock TTL is configurable** via `NORMCACHE_BUILDING_LOCK_TTL` (default: 30 s).
- Distributed write-lock removed; invalidation no longer requires a lock.

---

## [1.1.0] — 2026-05-20

### Added

- Query hit fast path: cached ID lists skip a round-trip when all model attributes are in cache.

### Changed

- Lua scripts overhauled for correctness and cluster compatibility.
- Cache keys are now connection-aware, preventing cross-connection collisions.

### Fixed

- Cache invalidation gaps for `through`-relation keys.
- Invalidations inside a transaction are flushed atomically on commit.
- `fresh()` / `refresh()` now bypasses cache, matching Laravel semantics.
- Laravel 11 / 12 compatibility fixes.
- Pivot and `through` cache accuracy improvements.

---

## [1.0.2] — 2026-05-14

### Added

- `CacheableBelongsTo`: warms `belongsTo` eager loads from the model cache, skipping a DB
  round-trip on hits.
- Primary-key fast paths for `whereInRaw`, `limit(0)`, and single-PK lookups.
- `NORMCACHE_FIRE_RETRIEVED`: opt-in to firing the Eloquent `retrieved` event on cache hits.

### Changed

- Model hydration rewritten for lower overhead.

---

## [1.0.1] — 2026-05-13

### Fixed

- **Cooldown staleness:** dropped invalidations are now lazily re-applied.
- **Pivot cache collisions:** pivot constraints are hashed into the key.
- Cache invalidations inside a transaction are deferred and applied atomically on commit.

---

## [1.0.0] — 2026-05-08

Initial release.
