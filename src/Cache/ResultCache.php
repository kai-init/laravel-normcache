<?php

namespace NormCache\Cache;

use Closure;
use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\ResultKind;
use NormCache\Payload\ResultAdapter;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Values\CacheConfig;
use NormCache\Values\CachePlan;
use NormCache\Values\PreparedQuery;

final class ResultCache
{
    public function __construct(
        private readonly VersionedPayloadStore $store,
        private readonly ResultAdapter $adapter,
        private readonly CacheConfig $config,
        private readonly CacheKeyBuilder $keys,
    ) {}

    public function execute(
        PreparedQuery $prepared,
        CachePlan $plan,
        ResultKind $kind,
        array $columns,
        Closure $compute,
    ): array {
        $builder = $prepared->builder;
        $model = $builder->getModel();
        $modelClass = $model::class;
        $connection = $model->getConnection()->getName()
            ?? $model->getConnectionName()
            ?? '';
        $tag = $builder->getCacheTag();
        $ttl = $builder->getQueryTtl();
        $debugbarStart = CacheReporter::beginMeasure();
        $namespace = $this->keys->namespaceFor(CacheKind::Result, $kind);
        $hash = $this->resolveHash($prepared, $kind, $columns);
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;
        $structuredPayload = $kind === ResultKind::Collection;
        $lockSuffix = $this->keys->resultBuildIdentityHash($namespace, $tag, $hash);

        $execution = CacheFallback::rescue(
            $this->config,
            function () use (
                $structuredPayload,
                $compute,
                $modelClass,
                $hash,
                $tag,
                $depClasses,
                $depTableKeys,
                $ttl,
                $connection,
                $lockSuffix,
                $debugbarStart,
                $kind,
                $plan,
            ) {
                $resolve = fn() => $this->store->getOrBuild(
                    adapter: $this->adapter,
                    build: fn() => $structuredPayload ? $compute() : [$compute()],
                    modelClass: $modelClass,
                    hash: $hash,
                    tag: $tag,
                    depClasses: $depClasses,
                    depTableKeys: $depTableKeys,
                    kind: CacheKind::Result,
                    resultKind: $kind,
                    ttl: $ttl,
                    connection: $connection,
                    lockSuffix: $lockSuffix,
                );

                $outcome = $resolve();

                if (!$structuredPayload
                    && $outcome->status === CacheStatus::Hit
                    && !array_key_exists(0, $outcome->payload)) {
                    $this->store->delete($outcome->key);
                    $outcome = $resolve();
                }

                $cached = $outcome->status === CacheStatus::Hit;

                if ($cached) {
                    CacheReporter::queryHit($modelClass, $outcome->key, $debugbarStart, [
                        ...CacheReporter::cacheMeta(CacheKind::Result, $outcome->status, $kind, $plan->space),
                        ...$outcome->meta,
                        'payload_shape' => $kind->value,
                    ]);
                } else {
                    CacheReporter::queryMiss($modelClass, $outcome->key, $debugbarStart, [
                        ...CacheReporter::cacheMeta(CacheKind::Result, $outcome->status, $kind, $plan->space),
                        ...$outcome->meta,
                        'payload_shape' => $kind->value,
                    ]);
                }

                return [
                    'value' => $structuredPayload ? $outcome->payload : $outcome->payload[0],
                    'cached' => $cached,
                ];
            },
            fn() => ['value' => $compute(), 'cached' => false],
        );

        return [$execution['value'], $execution['cached']];
    }

    private function resolveHash(PreparedQuery $prepared, ResultKind $kind, array $columns): string
    {
        $query = $prepared->base;

        if ($kind === ResultKind::Collection) {
            return QueryHasher::forResultQuery($prepared->builder, $prepared->baseWithColumns($columns));
        }

        return match ($kind) {
            ResultKind::PaginationCount => QueryHasher::forPaginationCountQuery($prepared->builder, $query),
            default => QueryHasher::forScalarQuery($prepared->builder, $query, $kind->value, $columns),
        };
    }
}
