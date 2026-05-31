<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use NormCache\CacheableBuilder;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!$this->shouldUseCache()) {
            return parent::get($columns);
        }

        $debugbarStart = NormCacheCollector::beginMeasure();

        $shouldCacheModels = $columns === ['*'] && $this->query->toBase()->columns === null;
        $builder = $this->prepareQueryBuilder($columns);
        $hash = QueryHasher::fromQuery($builder->toBase());
        $relatedClass = $this->related::class;

        try {
            $result = NormCache::getThroughCache($relatedClass, $this->throughParent::class, $hash);
            $key = $result['key'];

            if ($result['data'] !== null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheHit($relatedClass, $key));
                }

                NormCacheCollector::recordQuery('through hit', $relatedClass, $key, $debugbarStart, [
                    'through' => $this->throughParent::class,
                ]);

                return $this->hydrateFromIds(
                    $result['data']['ids'],
                    $relatedClass,
                    $builder,
                    $this->projectionColumns($shouldCacheModels),
                    $result['data']['throughKeys']
                );
            }

            if (NormCache::isEventsEnabled()) {
                event(new QueryCacheMiss($relatedClass, $key));
            }

            NormCacheCollector::recordQuery('through miss', $relatedClass, $key, $debugbarStart, [
                'through' => $this->throughParent::class,
            ]);

            $result = parent::get($columns);
            $payload = $this->cachePayloadFromResult($result);

            $modelAttrs = [];
            if ($shouldCacheModels) {
                foreach ($result as $model) {
                    $attrs = $model->getRawOriginal();
                    unset($attrs['laravel_through_key']);
                    $modelAttrs[$model->getKey()] = $attrs;
                }
            }

            NormCache::storeThroughResult($key, $payload, $relatedClass, $modelAttrs);

            return $result;
        } catch (\Exception $e) {
            NormCache::fallback($e);

            return parent::get($columns);
        }
    }

    private function shouldUseCache(): bool
    {
        return NormCache::isEnabled()
            && $this->parent->getConnection()->transactionLevel() === 0
            && !($this->query instanceof CacheableBuilder && $this->query->isCacheSkipped());
    }

    private function cachePayloadFromResult(Collection $result): array
    {
        return [
            'ids' => $result->modelKeys(),
            'throughKeys' => $result->mapWithKeys(fn($model) => [
                $model->getKey() => $model->getAttribute('laravel_through_key'),
            ])->all(),
        ];
    }

    private function projectionColumns(bool $shouldCacheModels): ?array
    {
        if ($shouldCacheModels) {
            return null;
        }

        $cols = $this->query->toBase()->columns;

        if ($cols === null || $cols === ['*']) {
            return null;
        }

        $hasWildcard = (bool) array_filter($cols, fn($c) => str_ends_with((string) $c, '*'));

        return $hasWildcard ? null : $cols;
    }

    private function hydrateFromIds(array $ids, string $relatedClass, Builder $builder, ?array $selectedColumns, array $throughKeys = []): Collection
    {
        $models = NormCache::getModels($ids, $relatedClass, $selectedColumns, null, $builder, false);

        if ($throughKeys !== []) {
            foreach ($models as $model) {
                if (array_key_exists($model->getKey(), $throughKeys)) {
                    $model->setAttribute('laravel_through_key', $throughKeys[$model->getKey()]);
                }
            }
        }

        if ($models && $builder->getEagerLoads()) {
            $models = $builder->eagerLoadRelations($models);
        }

        return $this->query->applyAfterQueryCallbacks(
            $this->related->newCollection($models)
        );
    }
}
