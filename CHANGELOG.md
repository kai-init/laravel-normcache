# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [2.4.0] — 2026-06-25

### Removed

- Dropped stale-serving and the `stale_version_depth` config option. It did shave latency off reads during a rebuild, but readers already wait on a wake channel in that window — the added complexity wasn't worth maintaining for that marginal win, so the build-lock path is simpler now.

### Fixed

- A malformed scalar/count/result cache entry (wrong shape for its kind) no longer recomputes from the database on every single read until TTL expiry — the first read after corruption now repairs the entry.

---

## [2.3.0] — 2026-06-24

### Added

- Pivot cache (`belongsToMany`/`morphToMany`) build-lock stampede protection: concurrent misses on the same batch wait on a wake signal instead of racing the database.
- `stampede_wake_tokens` config (default `64`): wakes N waiters per completed build instead of 1.
- Stale-serve (depth-bounded backward version search) extended from single-dependency queries to multi-dependency query, through, and result caches.

### Changed

- `fallback` default: `false` → `true` (fails open to the database on Redis errors). Set `NORMCACHE_FALLBACK=false` to restore fail-closed behavior.
- Removed `inline_model_threshold` config and the inline model-fetch path in the query/through Lua scripts; models now hydrate from cache in PHP after the Lua round trip.

### Fixed

- Cluster fixed-hash mode (`cluster=true`, `slotting=false`) now uses the atomic multi-key Lua path instead of the per-key fallback.
- `fetch_model_build_status.lua` chunks `MGET` at 500 keys/batch, fixing a Lua/Redis argument-limit error on cold misses ≥500 ids.
- Cold-miss re-query on joined tables now selects `{table}.*` instead of `*`, fixing an ambiguous-column error.

---

## [2.2.1] — 2026-06-23

### Fixed

- Relation definitions with built-in `whereExists`, raw predicates, or non-inferrable joins now bypass `whereHas`/`withCount` inference instead of caching under incomplete dependencies.
- `count()`, `exists()`, `sum()`, `avg()`, `min()`, `max()`, and `paginate()` with a manual `whereExists` now bypass unless `dependsOn()` is declared.
- `lockForUpdate()` and `withoutCache()` inside a `whereHas` constraint now prevent the outer query from being cached.
- Plain `join()` auto-inference extended to scalar and pagination paths, and now correctly bypasses on complex join predicates, implicit table aliases, or inside `withCount()`/`withAggregate()`.
- Through-relation `tag()` and `ttl()` now applied to result fetch and store calls.
- Pivot-relation `ttl()` now applied to result writes; `tag()` bypasses pivot caching (key structure does not support tag namespacing).
- Nested eager loads on a cached `belongsTo` relation (e.g. `with('order.customer')`) are now hydrated instead of being silently dropped.
- `latestOfMany()`/`oldestOfMany()`, `hasOne`, and `hasOneThrough` relations now correctly track their cache dependency.
- Fixed cross-slot `EVALSHA` errors when running against a real Redis Cluster with multi-dependency queries.

### Changed

- Reduced Redis round trips for cold cache misses across model hydration, relation loading, and the build-lock recheck.
- Cache hits on query/through/pivot lookups now decode their JSON payload in PHP instead of in Lua, avoiding Lua's slower bulk-reply marshaling on the warm path.

---

## [2.2.0] — 2026-06-16

### Added

- **Simple `whereHas`/`whereDoesntHave` caching:** `whereHas`, `orWhereHas`, `whereDoesntHave`, and `orWhereDoesntHave` are now cached automatically — no `dependsOn()` required. Works for single, non-nested `Cacheable` relations with safe constraint closures. Nested relations, `MorphTo`, and unsafe constraints still bypass.
- **Automatic join table inference:** plain `join()` calls automatically add the joined table as a cache dependency. Use an explicit root-table projection (e.g. `select('authors.*')`) to enable result caching; `SELECT *` joins still bypass.

### Fixed

- **Through-relation cross-parent cache collision:** `hasManyThrough`/`hasOneThrough` no longer share a cache key across different parent models. Previously two parents (e.g. two countries) could receive each other's cached results.
- **Through-relation `dependsOn()` dependencies ignored:** extra dependencies declared on a through-relation query are now included in the version keys and will correctly invalidate the cache.
- **Pivot relation with explicit `dependsOn()` bypasses instead of caching:** pivot caching cannot track extra dependencies in version keys, so it now bypasses rather than risk stale results.
- **Through simple guard misses non-canonical `from`:** the fast-path check now compares against the actual related model table, so aliased or replaced `from` sources are correctly detected and bypass the cache.

---

## [2.1.2] — 2026-06-15

### Fixed

- Fixed cache-key collisions between `sum`, `avg`, `min`, `max`, and `exists` aggregates.
- Through-relations with raw `where` bindings or subquery/`whereExists` predicates now bypass caching instead of risking collisions and stale reads.
- `value('column as alias')` no longer errors; it returns `null`, matching native Eloquent.
- `tag('')` is now rejected.
- Made the `normcache.events` default consistently `false` across config, provider, and README.
- Fixed the under-declared-dependency warning to reference `dependsOnTables()`.

### Changed

- `dependsOnTables()` now rejects reserved key characters in table names.
- `normcache:flush` output now says "NormCache key(s)" rather than "model cache key(s)".

---

## [2.1.1] — 2026-06-08

### Changed

- **BREAKING — cache events are now opt-in:** the `normcache.events` default changed from `true` to `false`.
- **Pivot cache batching:** batch pivot cache writes during eager loading to reduce Redis roundtrips.
- **Hydration optimization:** implemented closure-based hydration and removed expensive regex from unserialization for faster model loading.
- **Dedupe transaction bumps:** reduced redundant Redis calls by deduping version bumps within the same database transaction.
- **Reporting overhead:** model-hit key collection during result hydration is skipped when neither events nor a Debugbar collector are active.

### Fixed

- **Result cache correctness:** fixed edge cases where result cache could return stale or incorrect data.
- **Wildcard alias support:** improved handling of queries using wildcard aliases in the result cache.
- **Model hydration:** fixed issues with closure-based model hydration when restoring certain attribute types from cache.

---

## [2.1.0] — 2026-06-06

### Added

- **`dependsOnTables(array $tables)`:** declare raw table names as cache dependencies alongside `dependsOn()` model classes. Useful for queries that touch tables without a corresponding Cacheable model.
- **Dependency completeness warnings:** when `dependsOn()` or `dependsOnTables()` is used, the planner warns at query time if the declared dependencies do not cover all tables referenced in the query, surfacing likely invalidation gaps early.
- **`chunk()`, `each()`, and `lazy()` bypass cache:** streaming operations always execute against the database; caching partial result windows is not meaningful.
- **`sole()` bypasses cache:** must verify live row count against the database; a cached snapshot could incorrectly suppress or pass the uniqueness assertion.

### Changed

- **Query hash includes model casts:** queries on models with different cast configurations no longer share a cache key when the underlying SQL is identical.
- **Atomic version bump with TTL refresh:** version increments execute `INCR + EXPIRE` atomically, preventing counters from becoming permanent when a TTL-expired key is incremented.
- **Multi-dependency cluster routing:** normalized queries with more than one dependency model or table are routed to the result cache in cluster mode, where each version key resolves to its own slot.
- **Internal structure:** cache execution, column projection, query hashing, and Redis serialization have been reorganized for consistency across all cache paths.

### Fixed

- **`deleteQuietly()` invalidates cache:** now triggers the same flush as `delete()`.
- **`skipCache` propagation:** nested relation queries no longer inherit the flag through shared builder state.
- **Scalar corrupt-payload fallback:** a stored scalar with an unexpected shape falls back to a live query instead of returning a wrong value.
- **Relation projection accuracy:** through, pivot, and `belongsTo` relations with column constraints no longer write or read payloads built from the wrong column set.
- **Pivot constraint hash stability:** constraint bindings are now serialized to a stable representation across PHP versions.
- **`SSCAN` double-prefix:** member keys were being double-prefixed during model set flush scans.

---

## [2.0.1] — 2026-06-03

### Fixed

- **Cross-slot violations:** optional keys (building, wake) now share the primary hash tag so Lua scripts stay within a single slot.
- **Predis cluster script loading:** Predis cluster raises `NotSupportedException` instead of a NOSCRIPT error when a script isn't cached on a node; this is now caught and falls back to `EVAL`.
- **Connection prefix detection:** correctly reads the configured prefix for both standalone and cluster clients.
- **Transaction instance eviction:** a single-row save inside a transaction now evicts only that model's payload on commit instead of flushing the entire model class.
- **Result cache projection:** `get(['col'])` on a result-cached query now hashes and fetches with the correct column set instead of silently using `select *`.
- **Aggregate constraint dependencies:** extra model and table dependencies declared inside a `withCount`/`withSum` constraint are now merged into the aggregate dependency set.
- **Through/pivot projection guard:** `HasManyThrough`, `HasOneThrough`, and pivot relations now bypass cache when the related primary key is not in the selected columns.

### Removed

- **`remember()`** — replaced by `ttl()`. Use `->ttl(300)` to set a custom cache duration.

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
