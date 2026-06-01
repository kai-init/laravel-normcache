<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Arr;
use Illuminate\Support\LazyCollection;
use Illuminate\Support\Str;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Support\QueryInspector;
use NormCache\Traits\Cacheable;
use NormCache\Traits\CachesScalarResults;
use NormCache\Traits\HandlesCacheInvalidation;

class CacheableBuilder extends Builder
{
    use CachesScalarResults, HandlesCacheInvalidation;

    private static array $validatedModelClasses = [];

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private ?array $dependsOn = null;

    private bool $cacheAggregates = true;

    private array $pendingAggregates = [];

    private ?string $cacheTag = null;

    public function withoutCache(): static
    {
        $this->skipCache = true;

        return $this;
    }

    public function isCacheSkipped(): bool
    {
        return $this->skipCache;
    }

    public function remember(int $ttl): static
    {
        if ($ttl <= 0) {
            throw new \InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->queryTtl = $ttl;

        return $this;
    }

    public function withoutAggregateCache(): static
    {
        $this->cacheAggregates = false;

        if (!empty($this->pendingAggregates)) {
            $this->replayPendingAggregates();
        }

        return $this;
    }

    public function tag(string $tag): static
    {
        if (preg_match('/[:{}\s*]/', $tag)) {
            throw new \InvalidArgumentException('Cache tag must not contain reserved characters (: { } * or whitespace).');
        }

        $this->cacheTag = $tag;

        return $this;
    }

    public function dependsOn(array $modelClasses): static
    {
        if (empty($modelClasses)) {
            throw new \InvalidArgumentException('dependsOn() requires at least one model class.');
        }

        foreach ($modelClasses as $class) {
            if (!is_string($class)) {
                throw new \InvalidArgumentException('dependsOn() expects model class names, not model instances.');
            }

            if (isset(self::$validatedModelClasses[$class])) {
                continue;
            }

            if (!is_a($class, Model::class, true)) {
                throw new \InvalidArgumentException("dependsOn() class [{$class}] must be an Eloquent model.");
            }

            if (!in_array(Cacheable::class, class_uses_recursive($class), true)) {
                throw new \InvalidArgumentException(
                    "dependsOn() class [{$class}] must use the NormCache\\Traits\\Cacheable trait."
                );
            }

            self::$validatedModelClasses[$class] = true;
        }

        $this->dependsOn = $modelClasses;

        return $this;
    }

    // -------------------------------------------------------------------------
    // Public overrides
    // -------------------------------------------------------------------------

    public function withAggregate($relations, $column, $function = null): static
    {
        if (!$this->cacheAggregates) {
            return parent::withAggregate($relations, $column, $function);
        }

        $uncacheable = [];

        // mirrors Eloquent's alias formula exactly, including Expression unwrapping.
        $lowerFunction = strtolower((string) $function);
        $colValue = $this->getQuery()->getGrammar()->isExpression($column)
            ? $this->getQuery()->getGrammar()->getValue($column)
            : $column;

        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
                $constraint = null;
            }

            $explicitAlias = null;
            $segments = explode(' ', (string) $name);
            if (count($segments) === 3 && Str::lower($segments[1]) === 'as') {
                [$name, $explicitAlias] = [$segments[0], $segments[2]];
            }

            $alias = $explicitAlias ?? Str::snake(
                preg_replace('/[^[:alnum:][:space:]_]/u', '', "{$name} {$lowerFunction} {$colValue}")
            );

            $original = $explicitAlias !== null ? "{$name} as {$explicitAlias}" : $name;

            $entry = $this->classifyAggregate($name, $alias, $constraint, $lowerFunction, $colValue);

            if ($entry === null) {
                if ($constraint !== null) {
                    $uncacheable[$original] = $constraint;
                } else {
                    $uncacheable[] = $original;
                }

                continue;
            }

            $this->pendingAggregates[] = $entry;
        }

        if (!empty($uncacheable)) {
            parent::withAggregate($uncacheable, $column, $function);
        }

        return $this;
    }

    private function classifyAggregate(
        string $name,
        string $alias,
        ?callable $constraint,
        string $function,
        string $column,
    ): ?array {
        if (str_contains($name, '.')) {
            return null;
        }

        $relation = $this->model->{$name}();
        $relatedClass = $relation->getRelated()::class;

        if (!self::relatedIsCacheable($relatedClass)) {
            return null;
        }

        // Constraints using relation-specific APIs or cross-table reads can't have deps inferred.
        if ($constraint !== null) {
            try {
                $testBuilder = ($relatedClass)::withoutCache();
                $constraint($testBuilder);
                if (QueryInspector::hasDependencyBypass($testBuilder->toBase())) {
                    return null;
                }
            } catch (\Throwable) {
                return null;
            }
        }

        // Through model FK changes must invalidate the blob; skip non-Cacheable through models.
        $throughClass = null;
        if ($relation instanceof HasOneOrManyThrough) {
            $through = (new \ReflectionProperty($relation, 'throughParent'))->getValue($relation)::class;
            if (self::relatedIsCacheable($through)) {
                $throughClass = $through;
            }
        }

        return [
            'name' => $name,
            'alias' => $alias,
            'constraint' => $constraint,
            'function' => $function,
            'column' => $column,
            'relatedClass' => $relatedClass,
            'throughClass' => $throughClass,
        ];
    }

    public function explain(): string
    {
        $base = $this->toBase();
        $resolvedCols = QueryInspector::resolveSelectedColumns($base, ['*']);
        $grouped = $this->computeBypassReasons($base, $resolvedCols);

        if (empty($grouped)) {
            return $this->dependsOn !== null ? 'cached: dependsOn() opt-in' : 'cached';
        }

        if ($this->dependsOn !== null && !isset($grouped['safety']) && !isset($grouped['opted_out'])) {
            return 'cached: raw (dependsOn())';
        }

        $labels = QueryInspector::categoryLabels();
        $parts = [];
        foreach ($grouped as $category => $reasons) {
            $parts[] = ($labels[$category] ?? $category) . ': ' . implode(', ', $reasons);
        }

        return 'not cached — ' . implode(' | ', $parts);
    }

    public function get($columns = ['*']): Collection
    {
        if ($this->skipCache || !NormCache::isEnabled()) {
            return $this->getWithoutCache($columns);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $base = $this->toBase();
        $resolvedCols = QueryInspector::resolveSelectedColumns($base, (array) $columns);
        $model = $this->model::class;

        try {
            if ($this->shouldUseCache($base, $resolvedCols)) {
                if (!empty($this->pendingAggregates)) {
                    if ($this->queryReferencesPendingAggregateAlias($base)) {
                        return $this->getWithoutCache($columns);
                    }

                    return $this->getFromAggregateBlobCache($base, $model, $columns);
                }

                return $this->getFromCacheableQuery($base, $model, $resolvedCols);
            }

            if ($this->shouldUseRawCache($base)) {
                if (!empty($base->joins) && empty($base->columns)) {
                    if (config('app.debug')) {
                        logger()->warning('NormCache: dependsOn() JOIN without explicit select — added ' . $this->model->getTable() . '.* automatically.');
                    }

                    $base->select($this->model->getTable() . '.*');
                }

                // Replay into $base before hashing so aggregate subqueries are part of the blob key.
                if (!empty($this->pendingAggregates)) {
                    $this->replayPendingAggregates();
                }

                return $this->getFromRawCache($base, $model, $this->rawCacheKey($base), $this->cacheTag);
            }

            $bypassReasons = $this->computeBypassReasons($base, $resolvedCols);
            $result = $this->getDependencyOnlyBypassResult($base, $model, $resolvedCols, $bypassReasons);

            if ($result !== null) {
                return $result;
            }

            $this->recordBypass($model, $bypassReasons, $debugbarStart);

            return $this->getWithoutCache($columns);
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return $this->getWithoutCache($columns);
        }
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($total !== null || $this->skipCache || !NormCache::isEnabled()) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $base = $this->toBase();

        if ($this->shouldUseCache($base)) {
            return $this->paginateWithCountCache($base, 'count', $perPage, $columns, $pageName, $page, $total);
        }

        if ($this->shouldUseRawCache($base)) {
            return $this->paginateWithCountCache($base, 'raw count', $perPage, $columns, $pageName, $page, $total);
        }

        $bypassReasons = $this->computeBypassReasons($base);

        $this->recordBypass($this->model::class, $bypassReasons, $debugbarStart);

        return parent::paginate($perPage, $columns, $pageName, $page, $total);
    }

    public function eagerLoadRelations(array $models): array
    {
        if ($this->skipCache) {
            foreach ($this->eagerLoad as $name => $constraint) {
                $this->eagerLoad[$name] = function ($query) use ($constraint) {
                    $constraint($query);
                    $builder = $query instanceof EloquentRelation ? $query->getQuery() : $query;
                    if ($builder instanceof self) {
                        $builder->withoutCache();
                    }
                };
            }
        }

        return parent::eagerLoadRelations($models);
    }

    public function cursor(): LazyCollection
    {
        if (!empty($this->pendingAggregates)) {
            $this->replayPendingAggregates();
        }

        return parent::cursor();
    }

    public function lazy($chunkSize = 1000): LazyCollection
    {
        if (!empty($this->pendingAggregates)) {
            $this->replayPendingAggregates();
        }

        return parent::lazy($chunkSize);
    }

    // -------------------------------------------------------------------------
    // Private — query execution
    // -------------------------------------------------------------------------

    private function getFromAggregateBlobCache(QueryBuilder $base, string $model, mixed $columns): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();
        $depClasses = array_values(array_unique(array_merge(
            $this->dependsOn ?? [],
            $this->inferAggregateDependencies(),
        )));
        $hash = $this->aggregateBlobHash($base);

        $result = NormCache::getRawCache($model, $depClasses, $hash, $this->cacheTag);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild($model, $hash, depClasses: $depClasses, tag: $this->cacheTag);

            if ($result === null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheMiss($model, 'building:budget-exhausted'));
                }

                NormCacheCollector::recordQuery('query miss', $model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'agg-blob']);

                $this->replayPendingAggregates();

                return $this->finalizeResult(parent::get($columns)->all());
            }
        }

        if ($result['status'] === 'miss') {
            if (NormCache::isEventsEnabled()) {
                event(new QueryCacheMiss($model, $result['key']));
            }

            NormCacheCollector::recordQuery('query miss', $model, $result['key'], $debugbarStart, ['kind' => 'agg-blob']);

            $aggAliases = array_column($this->pendingAggregates, 'alias');
            $this->replayPendingAggregates();
            $models = parent::get($columns);
            // getRawOriginal() preserves $hidden; getAttribute(alias) applies dynamic casts (withExists → bool).
            $blob = $models->map(function ($m) use ($aggAliases) {
                $attrs = $m->getRawOriginal();
                foreach ($aggAliases as $alias) {
                    $attrs[$alias] = $m->getAttribute($alias);
                }

                return $attrs;
            })->all();
            NormCache::storeRawResult($result['key'], $blob, $result['buildingKey'], $this->queryTtl, $result['wakeKey'] ?? null);

            return $this->finalizeResult($models->all());
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $result['key']));
        }

        NormCacheCollector::recordQuery('query hit', $model, $result['key'], $debugbarStart, [
            'kind' => 'agg-blob',
            'contains' => class_basename($model) . ' (' . count($result['blob']) . ' models)',
        ]);

        $this->pendingAggregates = [];

        return $this->finalizeResult(NormCache::hydrateRaw($result['blob'], $model));
    }

    private function inferAggregateDependencies(): array
    {
        $classes = [];
        foreach ($this->pendingAggregates as $agg) {
            $classes[] = $agg['relatedClass'];
            if ($agg['throughClass'] ?? null) {
                $classes[] = $agg['throughClass'];
            }
        }

        return array_values(array_unique($classes));
    }

    private function aggregateBlobHash(QueryBuilder $base): string
    {
        $baseHash = $this->rawCacheKey($base);
        $specHashes = array_map(function ($agg) {
            $builder = ($agg['relatedClass'])::withoutCache();
            if ($agg['constraint'] !== null) {
                ($agg['constraint'])($builder);
            }

            return $agg['name'] . ':' . $agg['alias'] . ':' . $agg['function'] . ':' . $agg['column'] . ':' . QueryHasher::fromQuery($builder->toBase());
        }, $this->pendingAggregates);

        return sha1($baseHash . ':' . implode(':', $specHashes));
    }

    private function paginateWithCountCache(QueryBuilder $base, string $kind, $perPage, $columns, string $pageName, $page, $fallbackTotal): LengthAwarePaginator
    {
        try {
            return parent::paginate($perPage, $columns, $pageName, $page, $this->resolvePaginationTotal($base, $kind));
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return parent::paginate($perPage, $columns, $pageName, $page, $fallbackTotal);
        }
    }

    private function resolvePaginationTotal(QueryBuilder $base, string $kind): int
    {
        $queryStart = NormCacheCollector::beginMeasure();

        ['key' => $countKey, 'data' => $data] = NormCache::getNamespacedCache(
            'count',
            $this->model::class,
            $this->queryCacheKey($base),
            $this->dependsOn ?? [],
            $this->cacheTag
        );

        $cachedTotal = $data[0] ?? null;

        if (NormCache::isEventsEnabled()) {
            event($cachedTotal !== null
                ? new QueryCacheHit($this->model::class, $countKey)
                : new QueryCacheMiss($this->model::class, $countKey)
            );
        }

        NormCacheCollector::recordQuery(
            $cachedTotal !== null ? 'query hit' : 'query miss',
            $this->model::class,
            $countKey,
            $queryStart,
            ['kind' => $kind]
        );

        if ($cachedTotal === null) {
            $cachedTotal = $base->getCountForPagination();
            NormCache::storeQueryAggregate($countKey, $cachedTotal, $this->queryTtl);
        }

        return (int) $cachedTotal;
    }

    private function getByQuery(QueryBuilder $base, string $model, ?array $selectedCols): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();

        if ($this->dependsOn !== null) {
            return $this->getFromRawCache($base, $model, $this->rawCacheKey($base), $this->cacheTag);
        }

        $hash = $this->queryCacheKey($base);

        $result = NormCache::getModelsFromQuery($model, $hash, $this->cacheTag);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild($model, $hash, tag: $this->cacheTag);

            if ($result === null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheMiss($model, 'building:budget-exhausted'));
                }

                NormCacheCollector::recordQuery('query miss', $model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'ids']);

                return $this->finalizeResult(NormCache::getModels($this->buildIds($base), $model, $selectedCols, null, $this));
            }
        }

        if ($result['status'] === 'miss') {
            NormCacheCollector::recordQuery('query miss', $model, $result['key'], $debugbarStart, ['kind' => 'ids']);

            $ids = $this->resolveIds($result['key'], $base, $result['buildingKey'], $result['versionKeys'], $result['expectedVersions']);

            return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this));
        }

        $key = $result['status'] === 'stale' ? "stale:{$hash}" : $result['key'];

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $key));
        }

        NormCacheCollector::recordQuery('query hit', $model, $key, $debugbarStart, [
            'kind' => 'ids + models',
            'contains' => 'model hit: ' . class_basename($model) . ' (' . count($result['ids']) . ' ids)',
            'contains_model' => $result['ids'],
        ]);

        return $this->finalizeResult(NormCache::getModels($result['ids'], $model, $selectedCols, $result['models'], $this));
    }

    private function getFromRawCache(QueryBuilder $base, string $model, string $hash, ?string $tag = null): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();

        $result = NormCache::getRawCache($model, $this->dependsOn, $hash, $tag);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild($model, $hash, depClasses: $this->dependsOn, tag: $tag);

            if ($result === null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheMiss($model, 'building:budget-exhausted'));
                }

                NormCacheCollector::recordQuery('query miss', $model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'deps']);

                $blob = array_map(fn($r) => (array) $r, $base->get()->all());

                return $this->finalizeResult(NormCache::hydrateRaw($blob, $model, false));
            }
        }

        if ($result['status'] === 'miss') {
            if (NormCache::isEventsEnabled()) {
                event(new QueryCacheMiss($model, $result['key']));
            }

            NormCacheCollector::recordQuery('query miss', $model, $result['key'], $debugbarStart, ['kind' => 'deps']);

            $blob = array_map(fn($r) => (array) $r, $base->get()->all());
            NormCache::storeRawResult($result['key'], $blob, $result['buildingKey'], $this->queryTtl, $result['wakeKey'] ?? null);

            return $this->finalizeResult(NormCache::hydrateRaw($blob, $model, false));
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $result['key']));
        }

        NormCacheCollector::recordQuery('query hit', $model, $result['key'], $debugbarStart, [
            'kind' => 'raw',
            'contains' => class_basename($model) . ' (' . count($result['blob']) . ' models)',
        ]);

        return $this->finalizeResult(NormCache::hydrateRaw($result['blob'], $model));
    }

    private function getFromCacheableQuery(QueryBuilder $base, string $model, ?array $selectedCols): Collection
    {
        $ids = $this->extractPrimaryKeys($base);

        if ($ids !== null) {
            return $this->getModelsByIds($ids, $model, $selectedCols);
        }

        return $this->getByQuery($base, $model, $selectedCols);
    }

    /** @param array<string, list<string>> $bypassReasons */
    private function getDependencyOnlyBypassResult(QueryBuilder $base, string $model, ?array $selectedCols, array $bypassReasons): ?Collection
    {
        if (!$this->hasOnlyDependencyBypass($bypassReasons)) {
            return null;
        }

        $ids = $this->extractPrimaryKeys($base);

        return $ids === null ? null : $this->getModelsByIds($ids, $model, $selectedCols);
    }

    /** @param array<int, mixed> $ids */
    private function getModelsByIds(array $ids, string $model, ?array $selectedCols): Collection
    {
        return $this->finalizeResult(NormCache::getModels($ids, $model, $selectedCols, null, $this, false));
    }

    private function resolveIds(string $key, QueryBuilder $base, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = []): array
    {
        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheMiss($this->model::class, $key));
        }

        $ids = $this->buildIds($base);
        NormCache::storeQueryIds($key, $ids, $this->queryTtl, $buildingKey, $versionKeys, $expectedVersions);

        return $ids;
    }

    private function buildIds(QueryBuilder $base): array
    {
        return $base
            ->cloneWithout(['columns'])
            ->cloneWithoutBindings(['select'])
            ->select($this->model->getKeyName())
            ->pluck($this->model->getKeyName())
            ->all();
    }

    private function finalizeResult(array $models): Collection
    {
        if ($models && $this->eagerLoad) {
            $models = $this->eagerLoadRelations($models);
        }

        return $this->applyAfterQueryCallbacks($this->model->newCollection($models));
    }

    private function getWithoutCache($columns): Collection
    {
        $this->replayPendingAggregates();

        return parent::get($columns);
    }

    private function replayPendingAggregates(): void
    {
        foreach ($this->pendingAggregates as $agg) {
            $name = "{$agg['name']} as {$agg['alias']}";
            $relations = $agg['constraint'] !== null ? [$name => $agg['constraint']] : $name;

            parent::withAggregate($relations, $agg['column'], $agg['function']);
        }

        $this->pendingAggregates = [];
    }

    // -------------------------------------------------------------------------
    // Private — guards and key helpers
    // -------------------------------------------------------------------------

    /** @return array<string, list<string>> */
    private function computeBypassReasons(QueryBuilder $base, ?array $resolvedColumns = null): array
    {
        if ($this->skipCache) {
            return ['opted_out' => ['withoutCache() was called explicitly']];
        }

        if (!NormCache::isEnabled()) {
            return ['opted_out' => ['cache is globally disabled']];
        }

        if ($this->insideTransaction()) {
            return ['safety' => ['inside a database transaction']];
        }

        $reasons = QueryInspector::bypassReasons($base, $this->model->getTable(), $resolvedColumns);

        if ($this->dependsOn !== null) {
            unset($reasons['dependency']);
        }

        return $reasons;
    }

    private function shouldUseCache(QueryBuilder $base, ?array $resolvedColumns = null): bool
    {
        return !$this->insideTransaction()
            && QueryInspector::isStructurallyCacheable($base, $this->model->getTable(), $resolvedColumns)
            && ($this->dependsOn !== null || !QueryInspector::hasDependencyBypass($base));
    }

    private function shouldUseRawCache(QueryBuilder $base): bool
    {
        return $this->dependsOn !== null
            && !$this->insideTransaction()
            && !QueryInspector::hasSafetyBypass($base);
    }

    /** @param array<string, list<string>> $bypassReasons */
    private function hasOnlyDependencyBypass(array $bypassReasons): bool
    {
        return count($bypassReasons) === 1 && isset($bypassReasons['dependency']);
    }

    private function extractPrimaryKeys(QueryBuilder $base): ?array
    {
        return QueryInspector::extractPrimaryKeys($base, $this->model->getKeyName(), $this->model->getQualifiedKeyName());
    }

    private function recordBypass(string $modelClass, array $bypassReasons, ?float $debugbarStart): void
    {
        if (NormCache::isEventsEnabled()) {
            event(new QueryBypassed($modelClass, $bypassReasons));
        }

        NormCacheCollector::recordBypass($modelClass, $bypassReasons, $debugbarStart);
    }

    private static function relatedIsCacheable(string $class): bool
    {
        static $cache = [];

        return $cache[$class] ??= in_array(Cacheable::class, class_uses_recursive($class), true);
    }

    private function queryReferencesPendingAggregateAlias(QueryBuilder $base): bool
    {
        if (empty($this->pendingAggregates)) {
            return false;
        }

        $aliases = array_column($this->pendingAggregates, 'alias');

        foreach ($base->orders ?? [] as $clause) {
            if (isset($clause['column']) && in_array($clause['column'], $aliases, true)) {
                return true;
            }
            if (isset($clause['sql']) && $this->rawSqlReferencesAlias((string) $clause['sql'], $aliases)) {
                return true;
            }
        }

        foreach ($base->havings ?? [] as $clause) {
            if (isset($clause['column']) && in_array($clause['column'], $aliases, true)) {
                return true;
            }
            if (isset($clause['sql']) && $this->rawSqlReferencesAlias((string) $clause['sql'], $aliases)) {
                return true;
            }
        }

        foreach ($base->groups ?? [] as $group) {
            if (is_string($group) && in_array($group, $aliases, true)) {
                return true;
            }
        }

        foreach ($base->wheres as $clause) {
            if (isset($clause['column']) && in_array($clause['column'], $aliases, true)) {
                return true;
            }
            if (isset($clause['sql']) && $this->rawSqlReferencesAlias((string) $clause['sql'], $aliases)) {
                return true;
            }
        }

        return false;
    }

    private function rawSqlReferencesAlias(string $sql, array $aliases): bool
    {
        foreach ($aliases as $alias) {
            if (preg_match('/(?<![A-Za-z0-9_])' . preg_quote($alias, '/') . '(?![A-Za-z0-9_])/', $sql)) {
                return true;
            }
        }

        return false;
    }

    private function insideTransaction(): bool
    {
        return $this->model->getConnection()->transactionLevel() > 0;
    }

    public function applyRemovedScopesTo(self $target): void
    {
        foreach ($this->removedScopes as $scope) {
            $target->withoutGlobalScope($scope);
        }
    }

    public function hasRemovedScopes(): bool
    {
        return !empty($this->removedScopes);
    }

    private function queryCacheKey(QueryBuilder $base): string
    {
        $cols = $base->columns;
        $base->columns = null;
        try {
            return QueryHasher::fromQuery($base);
        } finally {
            $base->columns = $cols;
        }
    }

    private function rawCacheKey(QueryBuilder $base): string
    {
        return QueryHasher::fromQuery($base);
    }
}
