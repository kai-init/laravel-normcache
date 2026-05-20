<?php

namespace NormCache\Support;

use Closure;
use NormCache\CacheableBuilder;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;

final class ModelCacheLoader
{
    public function __construct(
        private readonly Closure $getMany,
        private readonly Closure $setManyModels,
        private readonly Closure $classKey,
        private readonly bool $fireRetrieved,
        private readonly bool $dispatchEvents,
        private readonly int $ttl,
    ) {}

    public function getModels(
        array $ids,
        string $modelClass,
        ?array $columns = null,
        ?array $raw = null,
        ?CacheableBuilder $missedQuery = null,
        bool $preserveQueryShape = true,
    ): array {
        if ($ids === []) {
            return [];
        }

        $classKey = ($this->classKey)($modelClass);

        if ($raw === null) {
            $keys = array_map(fn($id) => "model:{{$classKey}}:" . $id, $ids);
            $raw = ($this->getMany)($keys);
        }

        $projection = $columns !== null ? QueryInspector::normalizeProjection($columns) : null;
        ['hits' => $hits, 'missed' => $missed] = ModelHydrator::hydrate($ids, $modelClass, $raw, $projection, $this->fireRetrieved);

        if ($this->dispatchEvents && $hits !== []) {
            event(new ModelCacheHit($modelClass, array_keys($hits)));
        }

        if ($missed === []) {
            return array_values($hits);
        }

        if ($this->dispatchEvents) {
            event(new ModelCacheMiss($modelClass, $missed));
        }

        $fetched = $this->fetchFromDatabaseAndCache(
            $missed,
            $modelClass,
            $classKey,
            $projection,
            $missedQuery,
            $preserveQueryShape,
        );

        $ordered = [];
        foreach ($ids as $id) {
            if (isset($hits[$id]) || isset($fetched[$id])) {
                $ordered[] = $hits[$id] ?? $fetched[$id];
            }
        }

        return $ordered;
    }

    private function fetchFromDatabaseAndCache(
        array $missed,
        string $modelClass,
        string $classKey,
        ?array $projection,
        ?CacheableBuilder $missedQuery,
        bool $preserveQueryShape,
    ): array {
        $prototype = ModelHydrator::prototype($modelClass);
        $pk = $prototype->getKeyName();
        $qualifiedPk = $prototype->getQualifiedKeyName();
        $query = $this->prepareMissedQuery($modelClass, $missedQuery, $preserveQueryShape);
        $loaded = $query->whereIn($qualifiedPk, $missed)
            ->get(['*'])
            ->keyBy($pk);

        $attrsByKey = [];
        $deletedAtCol = ModelHydrator::deletedAtColumn($modelClass);

        foreach ($loaded as $id => $model) {
            $attrs = $model->getRawOriginal();

            $isTrashed = $deletedAtCol && isset($attrs[$deletedAtCol]);
            if (!$isTrashed) {
                $attrsByKey["model:{{$classKey}}:$id"] = $attrs;
            }

            if ($projection !== null) {
                $model->setRawAttributes(QueryInspector::projectAttributes($attrs, $projection), true);
            }
        }

        if ($attrsByKey !== []) {
            ($this->setManyModels)($modelClass, $attrsByKey, $this->ttl);
        }

        return $loaded->all();
    }

    private function prepareMissedQuery(string $modelClass, ?CacheableBuilder $missedQuery, bool $preserveQueryShape): CacheableBuilder
    {
        if ($missedQuery === null) {
            return $modelClass::query()->withoutCache();
        }

        $query = clone $missedQuery;
        $query->withoutCache();
        $query->setEagerLoads([]);
        $base = $query->getQuery()
            ->cloneWithout(['columns', 'orders', 'limit', 'offset'])
            ->cloneWithoutBindings(['select', 'order']);

        if (!$preserveQueryShape) {
            $base = $base
                ->cloneWithout(['joins', 'wheres'])
                ->cloneWithoutBindings(['join', 'where']);
        }

        $query->setQuery($base);

        return $query;
    }
}
