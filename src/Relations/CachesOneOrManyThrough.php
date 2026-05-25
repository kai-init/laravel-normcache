<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use NormCache\CacheableBuilder;
use NormCache\Debug\NormCacheCollector;
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
            $cacheData = NormCache::getThroughCache($relatedClass, $this->throughParent::class, $hash);
            $key = $cacheData['key'];

            if ($cacheData['data'] !== null) {
                NormCacheCollector::recordQuery('through hit', $relatedClass, $key, $debugbarStart, [
                    'through' => $this->throughParent::class,
                ]);

                return $this->hydrateFromIds(
                    $cacheData['data']['ids'],
                    $relatedClass,
                    $builder,
                    $shouldCacheModels ? null : $builder->getQuery()->columns,
                    $cacheData['data']['throughKeys']
                );
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
