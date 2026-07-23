<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Payload\ModelIndexAdapter;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Values\CachePlan;
use NormCache\Values\PreparedQuery;

final class ModelIndexCache
{
    public function __construct(
        private readonly VersionedPayloadStore $store,
        private readonly ModelIndexAdapter $adapter,
        private readonly ModelCache $models,
    ) {}

    public function getDirect(
        PreparedQuery $prepared,
        array $primaryKeys,
        string $model,
        ?array $selectedColumns,
        Model $prototype,
    ): Collection {
        return $prepared->finalizeModels($this->models->getModels(
            $primaryKeys,
            $model,
            $selectedColumns,
            null,
            $prepared->builder,
            false,
            $prototype,
        ));
    }

    public function get(
        PreparedQuery $prepared,
        CachePlan $plan,
        string $model,
        ?array $selectedColumns,
        ?string $cacheTag,
        ?int $queryTtl,
        ?float $debugbarStart,
        Model $prototype,
    ): Collection {
        $builder = $prepared->builder;
        $hash = QueryHasher::forModelIndexQuery($builder, $prepared->base);
        $connection = $prototype->getConnection()->getName()
            ?? $prototype->getConnectionName()
            ?? '';

        $outcome = $this->store->getOrBuild(
            adapter: $this->adapter,
            build: fn() => $this->buildIds($prepared->base, $prototype),
            modelClass: $model,
            hash: $hash,
            tag: $cacheTag,
            depClasses: $plan->dependencies->depClassesFor($model),
            depTableKeys: $plan->dependencies->tables,
            kind: CacheKind::ModelIndex,
            ttl: $queryTtl,
            connection: $connection,
        );

        $ids = $outcome->payload;
        $cached = $outcome->status === CacheStatus::Hit || $outcome->status === CacheStatus::Empty;

        if ($cached) {
            CacheReporter::queryHit($model, $outcome->key, $debugbarStart, [
                ...CacheReporter::cacheMeta(CacheKind::ModelIndex, $outcome->status, space: $plan->space),
                'payload_shape' => 'ids + models',
            ]);
        } else {
            CacheReporter::queryMiss($model, $outcome->key, $debugbarStart, [
                ...CacheReporter::cacheMeta(CacheKind::ModelIndex, $outcome->status, space: $plan->space),
                'payload_shape' => 'ids',
            ]);
        }

        $resolvedVersion = isset($outcome->build->expectedVersions[0])
            ? (int) $outcome->build->expectedVersions[0]
            : null;
        $raw = $resolvedVersion !== null
            ? $this->models->rawForVersion($model, $ids, $resolvedVersion, $connection)
            : null;

        return $prepared->finalizeModels($this->models->getModels(
            $ids,
            $model,
            $selectedColumns,
            $raw,
            $builder,
            true,
            $prototype,
            $resolvedVersion,
        ));
    }

    private function buildIds(QueryBuilder $query, Model $prototype): array
    {
        return $query
            ->cloneWithout(['columns'])
            ->cloneWithoutBindings(['select'])
            ->select($prototype->getKeyName())
            ->pluck($prototype->getKeyName())
            ->all();
    }
}
