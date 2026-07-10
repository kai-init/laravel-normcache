<?php

namespace NormCache\Cache;

use Closure;
use NormCache\Enums\ResultKind;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\QueryHasher;
use NormCache\Values\CacheConfig;
use NormCache\Values\CachePlan;
use NormCache\Values\PreparedQuery;
use NormCache\Values\ResultCacheResult;

final class ResultExecutor
{
    public function __construct(
        private readonly ExecutionEngine $engine,
        private readonly ResultCacheRepository $results,
        private readonly CacheConfig $config,
    ) {}

    public function execute(
        PreparedQuery $prepared,
        CachePlan $plan,
        ResultKind $kind,
        array $columns,
        Closure $compute,
    ): array {
        $builder = $prepared->builder;
        $modelClass = $builder->getModel()::class;
        $tag = $builder->getCacheTag();
        $ttl = $builder->getQueryTtl();
        $debugbarStart = CacheReporter::beginMeasure();

        $namespace = $this->resolveNamespace($kind);
        $hash = $this->resolveHash($prepared, $kind, $columns);
        $depClasses = $plan->dependencies->depClassesFor($modelClass);
        $depTableKeys = $plan->dependencies->tables;
        $structuredPayload = $kind === ResultKind::Collection;

        $execution = CacheFallback::rescue(
            $this->config,
            fn() => $this->engine->runScalar(
                fetch: fn() => $this->results->fetch($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace),
                waitForBuild: fn() => $this->results->waitForBuild($modelClass, $depClasses, $hash, $tag, $depTableKeys, $namespace),
                compute: fn() => ['value' => $compute(), 'cached' => false],
                onStore: function ($value, $result) use ($modelClass, $ttl, $debugbarStart, $kind) {
                    CacheReporter::queryMiss($modelClass, $result->key, $debugbarStart, ['kind' => $kind->value]);

                    $this->storeResult($result, $value['value'], $ttl);
                },
                onHit: function ($result) use ($modelClass, $debugbarStart, $kind, $compute, $structuredPayload, $ttl) {
                    if (!is_array($result->payload) || (!$structuredPayload && !array_key_exists(0, $result->payload))) {
                        $value = $compute();
                        $this->storeResult($result, $value, $ttl);

                        return ['value' => $value, 'cached' => false];
                    }

                    CacheReporter::queryHit($modelClass, $result->key ?? '', $debugbarStart, ['kind' => $kind->value]);

                    $value = $structuredPayload
                        ? $result->payload
                        : $result->payload[0];

                    return ['value' => $value, 'cached' => true];
                },
            ),
            fn() => ['value' => $compute(), 'cached' => false]
        );

        return [$execution['value'], $execution['cached']];
    }

    private function storeResult(ResultCacheResult $result, mixed $payload, ?int $ttl): void
    {
        $this->results->store(
            $result->key,
            is_array($payload) ? $payload : [$payload],
            $ttl,
            $result->build,
        );
    }

    private function resolveNamespace(ResultKind $kind): string
    {
        return match ($kind) {
            ResultKind::Count, ResultKind::PaginationCount => CacheKeyBuilder::K_COUNT,
            ResultKind::Collection => CacheKeyBuilder::K_RESULT,
            default => CacheKeyBuilder::K_SCALAR,
        };
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
