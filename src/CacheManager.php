<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use NormCache\Cache\ExecutionEngine;
use NormCache\Cache\ModelHydrator;
use NormCache\Cache\NormalizedCacheReader;
use NormCache\Cache\NormalizedThroughReader;
use NormCache\Cache\ResultCacheReader;
use NormCache\Cache\ResultExecutor;
use NormCache\Cache\VersionTracker;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FallbackHandler;
use NormCache\Support\RedisStore;
use NormCache\Traits\HandlesInvalidation;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheSpace;
use NormCache\Values\PivotCacheResult;
use NormCache\Values\QueryCacheResult;
use NormCache\Values\ResultCacheResult;
use NormCache\Values\ThroughCacheResult;

class CacheManager
{
    use HandlesInvalidation;

    public function __construct(
        private readonly NormalizedCacheReader $queryReader,
        private readonly ResultCacheReader $resultReader,
        private readonly NormalizedThroughReader $throughReader,
        private readonly ResultExecutor $result,
        private readonly ModelHydrator $hydrator,
        private readonly VersionTracker $versions,
        private readonly ExecutionEngine $engine,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly CacheConfig $config,
    ) {}

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    public function engine(): ExecutionEngine
    {
        return $this->engine;
    }

    public function result(): ResultExecutor
    {
        return $this->result;
    }

    public function config(): CacheConfig
    {
        return $this->config;
    }

    public function isEnabled(): bool
    {
        return $this->config->enabled;
    }

    public function isFallbackEnabled(): bool
    {
        return $this->config->fallbackEnabled;
    }

    public function isEventsEnabled(): bool
    {
        return $this->config->dispatchEvents;
    }

    public function enable(): void
    {
        $this->config->enabled = true;
    }

    public function disable(): void
    {
        $this->config->enabled = false;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public function getStore(): RedisStore
    {
        return $this->store;
    }

    public function keys(): CacheKeyBuilder
    {
        return $this->keys;
    }

    public function classKey(string $class): string
    {
        return $this->keys->classKey($class);
    }

    // Resolve the active cache space for a model (used by relations to scope their
    // cache execution to the same space the planner validated against).
    public function spaceFor(string $modelClass, ?string $explicitSpace = null): CacheSpace
    {
        return ($this->spaceResolver ??= app(CacheSpaceResolver::class))->resolve($modelClass, $explicitSpace);
    }

    public function withSpace(?CacheSpace $space, callable $callback): mixed
    {
        if ($space === null) {
            return $this->keys->withSpace(null, $callback);
        }

        $active = $this->keys->activeSpace();

        return $active !== null && $active->name === $space->name
            ? $callback()
            : $this->keys->withSpace($space, $callback);
    }

    public function withSpaceForModel(string $modelClass, ?string $explicitSpace, callable $callback): mixed
    {
        return $this->withSpace($this->spaceFor($modelClass, $explicitSpace), $callback);
    }

    private ?CacheSpaceResolver $spaceResolver = null;

    public function tableKey(string $connectionName, string $table): string
    {
        return $this->keys->tableKey($connectionName, $table);
    }

    public function currentVersion(string $modelClass): int
    {
        return $this->versions->currentVersion($modelClass, $this->modelSpaces($modelClass)[0]);
    }

    public function currentTableVersion(string $connectionName, string $table): int
    {
        return $this->versions->currentTableVersion($connectionName, $table);
    }

    // -------------------------------------------------------------------------
    // Reads
    // -------------------------------------------------------------------------

    public function getThroughCache(string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = []): ThroughCacheResult
    {
        return $this->throughReader->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);
    }

    public function getModelsFromQuery(string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = []): QueryCacheResult
    {
        return $this->queryReader->fetch($modelClass, $hash, $tag, $depClasses, $depTableKeys);
    }

    public function getPivotCache(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash = 'nc', ?string $pivotTableKey = null): PivotCacheResult
    {
        return $this->resultReader->fetchPivot($parentClass, $relatedClass, $relation, $parentIds, $constraintHash, $pivotTableKey);
    }

    public function waitForPivotBuild(string $parentClass, string $relatedClass, string $relation, array $parentIds, string $constraintHash, ?string $pivotTableKey): ?PivotCacheResult
    {
        return $this->resultReader->waitForPivotBuild($parentClass, $relatedClass, $relation, $parentIds, $constraintHash, $pivotTableKey);
    }

    public function getResultCache(string $modelClass, array $depClasses, string $hash, ?string $tag = null, array $depTableKeys = [], string $namespace = CacheKeyBuilder::K_RESULT): ResultCacheResult
    {
        return $this->resultReader->fetch($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace);
    }

    public function waitForQueryBuild(string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = []): ?QueryCacheResult
    {
        return $this->queryReader->waitForBuild($modelClass, $hash, $tag, $depClasses, $depTableKeys);
    }

    public function waitForThroughBuild(string $modelClass, string $hash, ?string $tag = null, array $depClasses = [], array $depTableKeys = []): ?ThroughCacheResult
    {
        return $this->throughReader->waitForBuild($modelClass, $hash, $tag, $depClasses, $depTableKeys);
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

    public function storeQueryIds(string $key, array $ids, ?int $ttl = null, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null, ?string $wakeKey = null): void
    {
        $this->queryReader->store($key, $ids, $ttl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken, $wakeKey);
    }

    public function storeThroughIds(string $key, array $ids, array $throughKeys, ?int $ttl = null, ?string $buildingKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null, ?string $wakeKey = null): bool
    {
        return $this->throughReader->store($key, $ids, $throughKeys, $ttl, $buildingKey, $versionKeys, $expectedVersions, $buildingToken, $wakeKey);
    }

    public function storeVersionedResult(string $key, mixed $payload, ?int $ttl = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null): bool
    {
        return $this->resultReader->storeEntry($key, $payload, $ttl ?? $this->config->queryTtl, $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken);
    }

    public function storeManyVersionedResults(array $entries, ?int $ttl = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingKey = null, ?string $wakeKey = null, ?string $buildingToken = null): bool
    {
        return $this->resultReader->storeMany($entries, $ttl ?? $this->config->queryTtl, $versionKeys, $expectedVersions, $buildingKey, $wakeKey, $buildingToken);
    }

    public function storeResultCache(string $key, array $payload, ?string $buildingKey, ?int $ttl, ?string $wakeKey = null, array $versionKeys = [], array $expectedVersions = [], ?string $buildingToken = null): bool
    {
        return $this->resultReader->store($key, $payload, $buildingKey, $ttl, $wakeKey, $versionKeys, $expectedVersions, $buildingToken);
    }

    public function storeModelAttrs(string $modelClass, array $modelAttrs, ?CacheSpace $space = null): void
    {
        $space ??= $this->keys->activeSpace();
        $modelVersion = $this->versions->currentVersion($modelClass, $space);

        $this->storeModelAttrsForVersion($modelClass, $modelAttrs, $modelVersion, $space);
    }

    private function expectedVersionForModel(string $modelClass, array $versionKeys, array $expectedVersions, ?CacheSpace $space = null): ?int
    {
        $classKey = $this->keys->classKey($modelClass);
        $index = array_search($this->keys->verKey($classKey, $space), $versionKeys, true);

        return $index === false || !isset($expectedVersions[$index])
            ? null
            : (int) $expectedVersions[$index];
    }

    public function storeModelAttrsForVersionedResult(
        string $modelClass,
        array $modelAttrs,
        array $versionKeys,
        array $expectedVersions,
        ?CacheSpace $space = null,
    ): void {
        $expectedVersion = $this->expectedVersionForModel($modelClass, $versionKeys, $expectedVersions, $space);

        if ($expectedVersion === null) {
            return;
        }

        $this->storeModelAttrsForVersion($modelClass, $modelAttrs, $expectedVersion, $space);
    }

    public function storeModelAttrsForVersion(string $modelClass, array $modelAttrs, int $expectedVersion, ?CacheSpace $space = null): void
    {
        if (empty($modelAttrs)) {
            return;
        }

        $classKey = $this->keys->classKey($modelClass);

        $attrsByKey = [];
        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey[$this->keys->modelPrefix($classKey, $expectedVersion, $space) . $id] = $attrs;
        }

        $this->store->setManyIfVersion(
            $attrsByKey,
            $this->config->ttl,
            $this->keys->verKey($classKey, $space),
            $expectedVersion
        );
    }

    // -------------------------------------------------------------------------
    // Flow
    // -------------------------------------------------------------------------
    public function rescue(callable $operation, callable $fallback): mixed
    {
        return FallbackHandler::rescue($this->config, $operation, $fallback);
    }

    public function attempt(callable $operation): bool
    {
        return FallbackHandler::attempt($this->config, $operation);
    }

    public function fallback(\Throwable $e): void
    {
        FallbackHandler::fallback($this->config, $e);
    }
}
