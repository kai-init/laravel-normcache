<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\CacheableBuilder;
use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\LuaStatus;
use NormCache\Support\AttributeProjector;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RawAttributes;
use NormCache\Support\RedisStore;
use NormCache\Values\BuildHandle;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheSpace;
use NormCache\Values\ModelFetchContext;

final class ModelCache
{
    private static array $overridesNewFromBuilder = [];

    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly VersionStore $versions,
        private readonly CacheConfig $config,
        private readonly bool $fireRetrieved,
        private readonly int $buildingLockTtl = 5,
        private readonly int $stampedeWaitMs = 200,
    ) {}

    public function getModels(
        array $ids,
        string $modelClass,
        ?array $columns = null,
        ?array $raw = null,
        ?CacheableBuilder $missedQuery = null,
        bool $preserveQueryShape = true,
        ?Model $prototype = null,
        ?int $resolvedVersion = null,
    ): array {
        if ($ids === []) {
            return [];
        }

        $reporting = CacheReporter::active();
        $startedAt = $reporting ? CacheReporter::beginMeasure() : null;
        $versionDeferred = $raw !== null && $resolvedVersion === null;
        $ids = array_values($ids);
        $connectionModel = $prototype ?? $missedQuery?->getModel() ?? CacheKeyBuilder::prototype($modelClass);
        $connection = $connectionModel->getConnection()->getName()
            ?? $connectionModel->getConnectionName()
            ?? '';
        $classKey = $this->keys->classKey($modelClass, $connection);
        $projection = $columns !== null ? AttributeProjector::normalizeProjection($columns) : null;

        if ($raw === null) {
            [$versionKey, $scheduledKey] = $this->keys->versionKeyPair($classKey);
            [$modelVersion, $raw] = $this->store->getManyForCurrentVersion(
                $versionKey,
                $scheduledKey,
                $this->keys->modelVersionPrefix($classKey),
                $ids,
            );
        } else {
            $modelVersion = $resolvedVersion ?? 0;
        }

        ['hits' => $hits, 'missed' => $missed, 'ordered' => $orderedHits] = $this->hydrateModelPayload(
            $ids,
            $modelClass,
            $raw,
            $projection,
            $prototype,
        );
        $repairCount = count($missed);

        if ($missed === []) {
            if ($reporting) {
                CacheReporter::modelHitActive($modelClass, $ids, $startedAt, [
                    ...CacheReporter::cacheMeta(CacheKind::Model, CacheStatus::Hit, space: $this->keys->activeSpace()),
                ]);
            }

            return $orderedHits;
        }

        $context = new ModelFetchContext(
            modelClass: $modelClass,
            classKey: $classKey,
            projection: $projection,
            prototype: $prototype,
            missedQuery: $missedQuery,
            preserveQueryShape: $preserveQueryShape,
            modelVersion: $modelVersion,
        );
        $context->hits = $hits;

        if ($context->hits !== [] && $reporting) {
            CacheReporter::modelHitActive($modelClass, array_keys($context->hits), $startedAt, [
                ...CacheReporter::cacheMeta(CacheKind::Model, CacheStatus::Hit, space: $this->keys->activeSpace()),
            ]);
        }

        if ($versionDeferred) {
            $context->modelVersion = $this->versions->currentVersion($modelClass, $this->keys->activeSpace(), $connection);
        }

        if ($reporting) {
            CacheReporter::modelMissActive($modelClass, $missed, $startedAt, [
                'hits' => array_keys($context->hits),
                'partial' => $context->hits !== [],
                'repair_count' => $repairCount,
                ...CacheReporter::cacheMeta(CacheKind::Model, CacheStatus::Miss, space: $this->keys->activeSpace()),
            ]);
        }

        [$context->lockKey, $context->wakeKey, $context->token] = $this->buildLockTriple($classKey, $context->modelVersion, $missed);
        [$status, $missed] = $this->fetchMissedStatus($missed, $context);

        if ($status === LuaStatus::Building && $missed !== []) {
            $this->store->brpop($context->wakeKey, $this->stampedeWaitMs / 1000.0);
            [$status, $missed] = $this->fetchMissedStatus($missed, $context);
        }

        if ($status === LuaStatus::Miss) {
            try {
                $this->fetchAndMerge($missed, $context, true);
            } catch (\Throwable $e) {
                $this->store->releaseBuilding($context->lockKey, $context->wakeKey, $context->token);

                throw $e;
            }
        } elseif ($status === LuaStatus::Hit && $missed !== []) {
            if ($this->store->setNxEx($context->lockKey, $context->token, $this->buildingLockTtl)) {
                try {
                    $this->fetchAndMerge($missed, $context, true);
                } catch (\Throwable $e) {
                    $this->store->releaseBuilding($context->lockKey, $context->wakeKey, $context->token);

                    throw $e;
                }
            } else {
                $this->fetchAndMerge($missed, $context, false);
            }
        } elseif ($status === LuaStatus::Building && $missed !== []) {
            $this->fetchAndMerge($missed, $context, false);
        }

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($context->hits[$id])) {
                $ordered[] = $context->hits[$id];
            }
        }

        if ($reporting) {
            CacheReporter::metric(
                'model_entry_repairs',
                $repairCount,
                CacheKind::Model,
                CacheStatus::Miss,
                $modelClass,
                space: $this->keys->activeSpace(),
            );
        }

        return $ordered;
    }

    public function rawForVersion(
        string $modelClass,
        array $ids,
        int $version,
        ?string $connection = null,
    ): array {
        if ($ids === []) {
            return [];
        }

        $classKey = $this->keys->classKey($modelClass, $connection);
        $prefix = $this->keys->modelPrefix($classKey, $version);

        return $this->store->getMany(array_map(
            static fn(mixed $id): string => $prefix . $id,
            $ids,
        ));
    }

    public function store(
        string $modelClass,
        array $modelAttrs,
        ?CacheSpace $space = null,
        ?string $connection = null,
    ): void {
        $space ??= $this->keys->activeSpace();
        $version = $this->versions->currentVersion($modelClass, $space, $connection);
        $this->storeForVersion($modelClass, $modelAttrs, $version, $space, $connection);
    }

    public function storeForBuild(
        string $modelClass,
        array $modelAttrs,
        BuildHandle $build,
        ?CacheSpace $space = null,
        ?string $connection = null,
    ): void {
        $classKey = $this->keys->classKey($modelClass, $connection);
        $index = array_search($this->keys->verKey($classKey, $space), $build->versionKeys, true);

        if ($index === false || !isset($build->expectedVersions[$index])) {
            return;
        }

        $this->storeForVersion(
            $modelClass,
            $modelAttrs,
            (int) $build->expectedVersions[$index],
            $space,
            $connection,
        );
    }

    public function storeForVersion(
        string $modelClass,
        array $modelAttrs,
        int $expectedVersion,
        ?CacheSpace $space = null,
        ?string $connection = null,
    ): void {
        if ($modelAttrs === []) {
            return;
        }

        $classKey = $this->keys->classKey($modelClass, $connection);
        $attrsByKey = [];

        foreach ($modelAttrs as $id => $attrs) {
            $attrsByKey[$this->keys->modelPrefix($classKey, $expectedVersion, $space) . $id] = $attrs;
        }

        $this->store->setManyIfVersion(
            $attrsByKey,
            $this->config->ttl,
            $this->keys->verKey($classKey, $space),
            $expectedVersion,
        );
    }

    public function hydrateResult(array $payload, Model $model, bool $cached = true): array
    {
        $modelClass = $model::class;
        $prototype = $model;
        $hydrate = RawAttributes::hydrateClosure();
        $models = [];

        foreach ($payload as $attrs) {
            $instance = clone $prototype;
            $hydrate($instance, $attrs, $this->fireRetrieved);
            $models[] = $instance;
        }

        if ($cached && $models !== [] && CacheReporter::active()) {
            $keys = [];
            foreach ($models as $instance) {
                if ($instance->getKey() !== null) {
                    $keys[] = $instance->getKey();
                }
            }
            CacheReporter::modelHit($modelClass, $keys, null, [
                ...CacheReporter::cacheMeta(CacheKind::Model, CacheStatus::Hit, space: $this->keys->activeSpace()),
            ]);
        }

        return $models;
    }

    public static function reset(): void
    {
        self::$overridesNewFromBuilder = [];
    }

    private function buildLockTriple(string $classKey, int $modelVersion, array $ids): array
    {
        $sorted = $ids;
        sort($sorted);
        $segment = 'model:v' . $modelVersion;
        $lockSuffix = $this->keys->resultBuildIdentityHash($segment, null, implode(',', $sorted));

        return [
            $this->keys->resultBuildingKey($classKey, $segment, $lockSuffix),
            $this->keys->wakeKey($classKey, $lockSuffix),
            $this->versions->buildLockToken(),
        ];
    }

    private function modelKeysFor(string $classKey, int $modelVersion, array $ids): array
    {
        $prefix = $this->keys->modelPrefix($classKey, $modelVersion);

        return array_map(static fn(mixed $id): string => $prefix . $id, $ids);
    }

    private function fetchMissedStatus(array $idsToFetch, ModelFetchContext $context): array
    {
        $result = $this->store->fetchBatchBuildStatus(
            $this->modelKeysFor($context->classKey, $context->modelVersion, $idsToFetch),
            $context->lockKey,
            $context->wakeKey,
            $context->token,
            $this->buildingLockTtl,
        );
        $raw = $this->store->unserializeMany($result[3] ?? []);
        ['hits' => $newHits, 'missed' => $stillMissed] = $this->hydrateModelPayload(
            $idsToFetch,
            $context->modelClass,
            $raw,
            $context->projection,
            $context->prototype,
        );

        foreach ($newHits as $id => $hit) {
            $context->hits[$id] = $hit;
        }

        if ($stillMissed === []) {
            return [LuaStatus::Hit, []];
        }

        return [LuaStatus::fromLua($result[0] ?? null), $stillMissed];
    }

    private function fetchAndMerge(array $missed, ModelFetchContext $context, bool $writeCache): void
    {
        if ($missed === []) {
            return;
        }

        foreach ($this->fetchFromDatabaseAndCache($missed, $context, $writeCache) as $id => $model) {
            $context->hits[$id] = $model;
        }
    }

    private function hydrateModelPayload(
        array $ids,
        string $modelClass,
        array $raw,
        ?array $projection,
        ?Model $prototype = null,
    ): array {
        $prototype ??= CacheKeyBuilder::prototype($modelClass);
        $hydrate = RawAttributes::hydrateClosure();
        $hits = [];
        $missed = [];
        $ordered = [];
        $seen = [];

        foreach ($ids as $index => $id) {
            if (isset($seen[$id])) {
                continue;
            }
            $seen[$id] = true;
            $attrs = $raw[$index] ?? null;

            if (!is_array($attrs)) {
                $missed[] = $id;

                continue;
            }

            if ($projection !== null) {
                $attrs = AttributeProjector::projectAttributes($attrs, $projection);
            }

            $instance = clone $prototype;
            $hydrate($instance, $attrs, $this->fireRetrieved);
            $hits[$id] = $instance;
            $ordered[] = $instance;
        }

        return ['hits' => $hits, 'missed' => $missed, 'ordered' => $ordered];
    }

    private function fetchFromDatabaseAndCache(array $missed, ModelFetchContext $context, bool $writeCache): array
    {
        $query = $this->prepareMissedQuery(
            $context->modelClass,
            $context->missedQuery,
            $context->preserveQueryShape,
        );

        return $this->overridesNewFromBuilder($query->getModel())
            ? $this->fetchAndCacheUsingEloquent($missed, $query, $context, $writeCache)
            : $this->fetchAndCacheUsingClosure($missed, $query, $context, $writeCache);
    }

    private function fetchAndCacheUsingEloquent(
        array $missed,
        EloquentBuilder $query,
        ModelFetchContext $context,
        bool $writeCache,
    ): array {
        $prototype = CacheKeyBuilder::prototype($context->modelClass);
        $primaryKey = $prototype->getKeyName();
        $loaded = $query->whereIn($prototype->getQualifiedKeyName(), $missed)
            ->get([$prototype->getTable() . '.*']);
        $hydrate = $context->projection !== null ? RawAttributes::hydrateClosure() : null;
        $models = $this->cacheAndCollect($loaded, $context, $writeCache, function ($model) use ($primaryKey, $hydrate, $context) {
            $attrs = $model->getRawOriginal();

            if ($hydrate !== null) {
                $hydrate($model, AttributeProjector::projectAttributes($attrs, $context->projection), false);
            }

            return array_key_exists($primaryKey, $attrs) ? [$attrs[$primaryKey], $attrs, $model] : null;
        });

        return $models;
    }

    private function fetchAndCacheUsingClosure(
        array $missed,
        EloquentBuilder $query,
        ModelFetchContext $context,
        bool $writeCache,
    ): array {
        $prototype = $query->getModel();
        $primaryKey = $prototype->getKeyName();
        $hydrate = RawAttributes::hydrateClosure();
        $connectionName = $prototype->getConnectionName();
        $rows = $query->whereIn($prototype->getQualifiedKeyName(), $missed)
            ->toBase()
            ->get([$prototype->getTable() . '.*']);
        $models = $this->cacheAndCollect($rows, $context, $writeCache, function ($row) use ($primaryKey, $prototype, $hydrate, $connectionName, $context) {
            $attrs = (array) $row;

            if (!array_key_exists($primaryKey, $attrs)) {
                return null;
            }

            $returnAttrs = $context->projection !== null
                ? AttributeProjector::projectAttributes($attrs, $context->projection)
                : $attrs;
            $instance = clone $prototype;
            $instance->setConnection($connectionName);
            $hydrate($instance, $returnAttrs, true);

            return [$attrs[$primaryKey], $attrs, $instance];
        });

        return $models;
    }

    private function cacheAndCollect(
        iterable $rows,
        ModelFetchContext $context,
        bool $writeCache,
        callable $each,
    ): array {
        $deletedAtColumn = CacheKeyBuilder::deletedAtColumn($context->modelClass);
        $prefix = $this->keys->modelPrefix($context->classKey, $context->modelVersion);
        $attrsByKey = [];
        $models = [];

        foreach ($rows as $row) {
            $result = $each($row);

            if ($result === null) {
                continue;
            }

            [$id, $attrs, $model] = $result;

            if ($writeCache && !($deletedAtColumn && isset($attrs[$deletedAtColumn]))) {
                $attrsByKey[$prefix . $id] = $attrs;
            }

            $models[$id] = $model;
        }

        $this->storeModelAttrs($attrsByKey, $context, $writeCache);

        return $models;
    }

    private function storeModelAttrs(array $attrsByKey, ModelFetchContext $context, bool $writeCache): void
    {
        $this->store->setManyIfVersion(
            $attrsByKey,
            $this->config->ttl,
            $this->keys->verKey($context->classKey),
            $context->modelVersion,
            $writeCache ? $context->lockKey : null,
            $writeCache ? $context->wakeKey : null,
            $writeCache ? $context->token : null,
        );
    }

    private function overridesNewFromBuilder(Model $model): bool
    {
        $class = $model::class;

        return self::$overridesNewFromBuilder[$class] ??=
            (new \ReflectionMethod($model, 'newFromBuilder'))->getDeclaringClass()->getName() !== Model::class;
    }

    private function prepareMissedQuery(
        string $modelClass,
        ?CacheableBuilder $missedQuery,
        bool $preserveQueryShape,
    ): EloquentBuilder {
        if ($missedQuery === null || !$preserveQueryShape || !$this->canPreserveQueryShape($missedQuery->getQuery())) {
            /** @var CacheableBuilder $builder */
            $builder = $missedQuery !== null
                ? $missedQuery->getModel()->newQuery()
                : $modelClass::query();

            if ($missedQuery !== null) {
                $builder->withoutGlobalScopes($missedQuery->removedScopes());
            }

            return $builder->withoutCache();
        }

        $base = $missedQuery->getQuery()
            ->cloneWithout(['columns', 'orders', 'limit', 'offset', 'groups', 'havings'])
            ->cloneWithoutBindings(['select', 'order', 'groupBy', 'having']);
        $builder = (new CacheableBuilder($base))
            ->setModel($missedQuery->getModel())
            ->withoutCache();
        $builder->withoutGlobalScopes($missedQuery->removedScopes());

        return $builder;
    }

    private function canPreserveQueryShape(QueryBuilder $base): bool
    {
        return $base->unions === null && is_string($base->from);
    }
}
