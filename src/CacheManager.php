<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use NormCache\Cache\CacheFlowGuard;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\NormalizedCacheReader;
use NormCache\Cache\CacheExecutor;
use NormCache\Cache\ResultCacheReader;
use NormCache\Cache\VersionTracker;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Traits\HandlesInvalidation;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ResultCacheResult;

class CacheManager
{
    use HandlesInvalidation;

    public function __construct(
        private readonly NormalizedCacheReader $queryReader,
        private readonly ResultCacheReader $resultReader,
        private readonly ModelHydrator $hydrator,
        private readonly VersionTracker $versions,
        private readonly CacheFlowGuard $guard,
        private readonly CacheExecutor $executor,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private int $ttl,
        private int $queryTtl,
        private int $cooldown,
        private bool $enabled = true,
        private bool $dispatchEvents = true,
        private bool $cluster = false,
        private bool $slotting = false,
    ) {}

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    public function executor(): CacheExecutor
    {
        return $this->executor;
    }

    public function isEnabled(): bool
    {
        return $this->enabled && $this->guard->isEnabled();
    }

    public function isEventsEnabled(): bool
    {
        return $this->dispatchEvents;
    }

    public function isCluster(): bool
    {
        return $this->cluster;
    }

    public function isSlotting(): bool
    {
        return $this->slotting;
    }

    public function enable(): void
    {
        $this->enabled = true;
        $this->guard->enable();
    }

    public function disable(): void
    {
        $this->enabled = false;
        $this->guard->disable();
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public function getStore(): RedisStore
    {
        return $this->store;
    }

    public function classKey(string $class): string
    {
        return $this->keys->classKey($class);
    }

    public function tableKey(string $connectionName, string $table): string
    {
        return $this->keys->tableKey($connectionName, $table);
    }

    public function currentVersion(string $modelClass): int
    {
        return $this->versions->currentVersion($modelClass);
    }

    public function currentTableVersion(string $connectionName, string $table): int
    {
        return $this->versions->currentTableVersion($connectionName, $table);
    }

    // -------------------------------------------------------------------------
    // Reads
    // -------------------------------------------------------------------------

    public function getModelsFromQuery(string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = []): QueryCacheResult
    {
        return $this->queryReader->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc', ?string $pivotTableKey = null): PivotCacheResult
    {
        return $this->resultReader->fetchPivot($parentClass, $relatedClass, $relation, $parentIds, $constraintHash, $pivotTableKey);
    }

    public function getResultCache(string $modelClass, array $depClasses, string $hash, ?string $tag = null, array $depTableKeys = [], string $namespace = CacheKeyBuilder::K_RESULT): ResultCacheResult
    {
        return $this->resultReader->fetch($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);
    }

    public function waitForBuild(string $store, string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = [], string $namespace = CacheKeyBuilder::K_RESULT): mixed
    {
        if ($store !== 'result') {
            return $this->queryReader->waitForBuild($modelClass, $hash, $tag, $depClasses, $depTableKeys);
        }

        return $this->resultReader->waitForBuild($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);
    }

    // -------------------------------------------------------------------------
    // Loading
    // -------------------------------------------------------------------------

    public function getModels(
        array $ids,
        string $modelClass,
        ?array $columns = null,
        ?array $raw = null,
        ?EloquentBuilder $missedQuery = null,
        bool $preserveQueryShape = true,
        ?Model $prototype = null,
    ): array {
        return $this->hydrator->getModels($ids, $modelClass, $columns, $raw, $missedQuery, $preserveQueryShape, $prototype);
    }

    public function hydrateResult(array $payload, string|Model $model, bool $cached = true): array
    {
        return $this->hydrator->hydrateResult($payload, $model, $cached);
    }

    // -------------------------------------------------------------------------
    // Storage
    // -------------------------------------------------------------------------

    public function storeQueryIds(string $key, array $ids, ?int $ttl = null, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): void
    {
        $this->queryReader->store($key, $ids, $ttl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken);
    }

    public function storeVersionedResult(string $key, mixed $payload, ?int $ttl = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null): bool
    {
        return $this->resultReader->storeEntry($key, $payload, $ttl ?? $this->queryTtl, $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken);
    }

    public function storeResultCache(string $key, array $payload, ?string $buildingKey, ?int $ttl, ?string $wakeKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): bool
    {
        return $this->resultReader->store($key, $payload, $buildingKey, $ttl, $wakeKey, $versionKeys, $expectedVersions, $buildingToken);
    }

    public function cacheModelAttrs(string $modelClass, array $modelAttrs): void
    {
        if (empty($modelAttrs)) {
            return;
        }

        $classKey = $this->keys->classKey($modelClass);
        $modelVersion = $this->currentVersion($modelClass);

        $attrsByKey = [];
        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey[$this->keys->modelPrefix($classKey) . $id] = $attrs;
        }

        $this->store->setManyTrackedIfVersion(
            $attrsByKey,
            $this->ttl,
            $this->keys->membersKey($classKey),
            $this->keys->verKey($classKey),
            $modelVersion
        );
    }

    // -------------------------------------------------------------------------
    // Flow
    // -------------------------------------------------------------------------
    public function rescue(callable $operation, callable $fallback): mixed
    {
        return $this->guard->rescue($operation, $fallback);
    }

    public function attempt(callable $operation): bool
    {
        return $this->guard->attempt($operation);
    }

    public function fallback(\Throwable $e): void
    {
        $this->guard->fallback($e);
    }
}
