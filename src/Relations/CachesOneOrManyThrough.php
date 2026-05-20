<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

trait CachesOneOrManyThrough
{
    public function get($columns = ['*']): Collection
    {
        if (!$this->shouldUseCache()) {
            return parent::get($columns);
        }

        $shouldCacheModels = $columns === ['*'] && $this->query->toBase()->columns === null;
        $builder = $this->prepareQueryBuilder($columns);
        $hash = QueryHasher::fromQuery($builder->toBase());
        $relatedClass = $this->related::class;
        $lockKey = null;

        try {
            $cacheData = NormCache::getThroughCache($relatedClass, $this->throughParent::class, $hash);
            $key = $cacheData['key'];
            $lockKey = $cacheData['lock'];

            if ($cacheData['data'] !== null) {
                return $this->hydrateFromIds(
                    $cacheData['data']['ids'],
                    $relatedClass,
                    $builder,
                    $shouldCacheModels ? null : $builder->getQuery()->columns,
                    $cacheData['data']['throughKeys']
                );
            }

            if ($lockKey === null) {
                $payload = NormCache::poll(fn() => NormCache::get($key));
                if ($payload !== null) {
                    return $this->hydrateFromIds(
                        $payload['ids'],
                        $relatedClass,
                        $builder,
                        $shouldCacheModels ? null : $builder->getQuery()->columns,
                        $payload['throughKeys']
                    );
                }
            }

            $result = parent::get($columns);
            $payload = $this->cachePayloadFromResult($result);

            if ($lockKey === null) {
                NormCache::set($key, $payload, NormCache::queryTtl());
            } else {
                NormCache::setAndReleaseLock($key, $payload, NormCache::queryTtl(), $lockKey);
            }

            $lockKey = null;

            if ($shouldCacheModels) {
                $attrsByKey = [];
                foreach ($result as $model) {
                    $attrs = $model->getRawOriginal();
                    unset($attrs['laravel_through_key']);
                    $attrsByKey[NormCache::modelKey($relatedClass, $model->getKey())] = $attrs;
                }
                if (!empty($attrsByKey)) {
                    NormCache::setManyModels($relatedClass, $attrsByKey, NormCache::ttl());
                }
            }

            return $result;
        } catch (\Exception $e) {
            NormCache::triggerFallback($e);

            return parent::get($columns);
        } finally {
            if ($lockKey !== null) {
                try { NormCache::delete($lockKey); } catch (\Exception) {}
            }
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
