# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [2.0.0] — Unreleased

### Added

- **`dependsOn(array $modelClasses)`** — declare that a query's result depends on models beyond
  its own table. The cache invalidates the entry whenever any of the listed model classes are
  invalidated, making cross-table and `whereHas`/`whereExists` queries safe to cache. Only the
  *dependency* bypass category is suppressed; normalization and safety checks (e.g. `GROUP BY`,
  `DISTINCT`, pessimistic locks) still apply and will bypass the cache when triggered.
- **Scalar result caching.** Simple aggregate queries (`count`, `sum`, `avg`, `min`, `max`) are
  now cached and invalidated alongside their parent model, including `dependsOn()`-aware keys
  for cross-table scalars.
- **`Builder::explain()`.** Returns a human-readable string describing why a query is cached or
  which bypass category prevented caching — useful for debugging and development.
- **Debugbar integration.** Cache hits, misses, and bypasses are recorded on the Laravel Debugbar
  timeline when the package is present. The integration is optional — NormCache degrades gracefully
  when Debugbar is absent.

### Fixed

- **`flushAll()` no longer blocks Redis with `KEYS` on non-cluster connections.**
  `RedisStore::keysForPattern()` now uses a `SCAN` cursor loop for all connection types,
  matching the behaviour that was already in place for cluster connections. Predis cluster
  connections additionally gain per-node `SCAN` iteration.

- **`forceFlushModel()` no longer loads the entire member-tracking set into PHP memory.**
  Replaced the `SMEMBERS` call with an `SSCAN` cursor loop that deletes members in batches
  of 1 000, keeping memory usage constant regardless of how many model entries are tracked.

- **Model-cache writes no longer reintroduce stale entries after a concurrent invalidation.**
  `flushInstance()` and `forceFlushModel()` now bump the model version before deleting tracked
  model keys. Subsequent model-cache writes captured under the old version are rejected via a
  Lua compare-and-swap check (`setManyTrackedIfVersion`), preventing stale entries from
  re-entering the cache after a flush.

- **Query-cache writes are skipped when the version has been bumped since the build started.**
  A Lua CAS script atomically checks all relevant version keys before writing the ID list.
  The building lock is always released, even when the write is skipped.

- **Octane: per-request flush queue is now discarded between requests.**
  Previously, deferred invalidations that were not flushed in one request could leak into the
  next. The Octane lifecycle listener now calls `discardAllPending()` before re-enabling the
  cache for each new request.

- **igbinary format detection via magic-header byte.**
  `RedisStore::unserialize()` now inspects the first byte of the stored blob to detect the
  serialization format rather than relying solely on the runtime `igbinary` flag. Workers
  without igbinary installed return a cache miss (null) instead of a corrupt result when
  encountering an igbinary-serialized entry.

- Hit events are now fired correctly on stale-key cache hits.
- Nested eager loads on pivot cache hits no longer fail to hydrate.
- Fixed non-sequential array key handling when hydrating relations.
- Fixed new-model cache flush that left stale entries after `create()`.
- Fixed broken public `flushModel()` API introduced in v1.1.0.

### Changed

- **Building-lock TTL is now configurable** via `NORMCACHE_BUILDING_LOCK_TTL` (default: 30 seconds).
- Removed write-lock / polling mechanism; invalidation is now handled without distributed locks.
- Non-cluster Redis paths optimised to reduce round-trips.
- `RedisStore` extracted as a standalone class; cache operations are now named and scoped.

---

## [1.1.0] — 2026-05-20

### Added

- Query hit fast path restored: cached ID lists are returned without a redundant round-trip
  when all model attributes are already in cache.

### Changed

- Lua scripts overhauled for correctness and cluster compatibility.
- Cache loading flow refactored to reduce internal complexity; `CacheManager` now accepts
  model instances rather than class strings in more call sites.
- Cache keys are now runtime-connection-aware, preventing cross-connection key collisions.
- Improved accuracy of the query-cache parity check (alias handling, `fromRaw`, `havings`).

### Fixed

- Cache invalidation gaps for `through`-relation keys and `members` TTL alignment.
- Bulk-write transaction deferral: invalidations queued inside a transaction are now flushed
  atomically on commit.
- Cache accuracy regressions for aliased `FROM`, pending flushes, and `classKey` consistency.
- `fresh()` / `refresh()` now correctly bypasses cache, matching Laravel's own semantics.
- Laravel 11 / 12 compatibility fixes.
- Pivot and `through` cache accuracy improvements.
- Connection-aware cache keys for selected-column variance.

---

## [1.0.2] — 2026-05-14

### Added

- `CacheableBelongsTo` relation: warms simple `belongsTo` eager loads directly from the model
  cache, skipping a query round-trip on cache hits.
- Primary-key fast paths for `whereInRaw`, `limit(0)`, and single-PK lookups with `limit(1)`
  or a harmless `orderBy`.
- `NORMCACHE_FIRE_RETRIEVED` config option: opt-in to firing the Eloquent `retrieved` event
  during cache hydration.

### Changed

- Model hydration rewritten using bound closures and prototype hydration for lower overhead.
- Non-cluster `MGET` handling simplified; model member `SADD` is now batched in a pipeline.

---

## [1.0.1] — 2026-05-13

### Fixed

- **Cooldown staleness:** Dropped invalidations during a cooldown window are now lazily
  re-applied, preventing the cache from getting permanently stuck on stale data.
- **Pivot cache collisions:** Pivot constraints are now hashed into the cache key, so different
  filters on the same relationship no longer overwrite each other.
- Improved transaction safety: cache invalidations are deferred and applied atomically on commit.
- Prevented accidental mutation of the caller's base query builder.
- Minor bug fixes and internal cleanups.

---

## [1.0.0] — 2026-05-08

Initial release.
