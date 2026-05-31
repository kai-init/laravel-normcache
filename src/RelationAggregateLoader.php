<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\QueryHasher;

class RelationAggregateLoader
{
    private static array $cache = [];

    public function __construct(
        private Model $model,
        private ?CacheableBuilder $parentBuilder,
        private ?string $cacheTag,
        private CacheKeyBuilder $keys,
    ) {
        $this->keys = new CacheKeyBuilder;
    }

    public function load(array $models, array $pendingAggregates): array
    {
        if (empty($models)) {
            return $models;
        }

        $parentClass = $this->model::class;

        $pkName = $this->model->getKeyName();
        $prefix = CacheKeyBuilder::K_AGG . ':{' . NormCache::classKey($parentClass) . '}:' . $this->keys->tagSegment($this->cacheTag);

        $ids = array_map(fn($m) => $m->getKey(), $models);
        $idCount = count($ids);

        $relations = [];
        $luaSpecs = [];

        foreach ($pendingAggregates as $agg) {
            ['name' => $name, 'constraint' => $constraint, 'function' => $function, 'column' => $column] = $agg;

            if (!isset($relations[$name])) {
                $relations[$name] = $this->model->{$name}();
            }

            $relation = $relations[$name];
            $relatedClass = $relation->getRelated()::class;

            $secondClass = null;
            $secondLabel = '';
            if ($relation instanceof BelongsTo) {
                $secondClass = $relation->getParent()::class;
                $secondLabel = 'p';
            } elseif ($relation instanceof HasOneOrManyThrough) {
                $secondClass = $relation->getParent()::class;
                $secondLabel = 't';
            }

            $luaSpecs[] = [
                'staticSuffix' => ':' . $column . ':' . $function . ':' . $name . ':' . $this->constraintKey($relatedClass, $constraint),
                'relatedClass' => $relatedClass,
                'secondClass' => $secondClass,
                'secondLabel' => $secondLabel,
            ];
        }

        $debugbarStart = NormCacheCollector::beginMeasure();
        ['data' => $data, 'suffixes' => $verSuffixes] = NormCache::fetchVersionedAggregates($prefix, $ids, $luaSpecs);

        $toCache = [];
        $offset = 0;

        $hydrator = self::$cache['hydrator'] ??= \Closure::bind(static function ($model, $key, $value) {
            $model->attributes[$key] = $value;
            $model->original[$key] = $value;
        }, null, Model::class);

        foreach ($pendingAggregates as $i => $agg) {
            ['name' => $name, 'constraint' => $constraint, 'function' => $function, 'column' => $column] = $agg;
            $alias = $agg['alias'];
            $suffix = $luaSpecs[$i]['staticSuffix'] . $verSuffixes[$i];

            $missed = [];
            $cachedValues = [];

            foreach ($ids as $index => $id) {
                $cached = $data[$offset + $index];
                if (!is_array($cached)) {
                    $missed[] = $id;

                    continue;
                }

                $cachedValues[$id] = $cached['v'];
            }

            $fetched = [];
            if (!empty($missed)) {
                $fetched = $this->fetchMissed($missed, "{$name} as {$alias}", $constraint, $function, $column, $pkName, $alias);

                foreach ($missed as $id) {
                    $toCache["{$prefix}{$id}{$suffix}"] = ['v' => $fetched[$id] ?? null];
                }
            }

            $cacheKey = "{$prefix}*{$suffix}";
            $hit = $missed === [];

            if (NormCache::isEventsEnabled()) {
                event($hit
                    ? new QueryCacheHit($parentClass, $cacheKey)
                    : new QueryCacheMiss($parentClass, $cacheKey)
                );
            }

            NormCacheCollector::recordQuery(
                $hit ? 'query hit' : 'query miss',
                $parentClass,
                $cacheKey,
                $debugbarStart,
                [
                    'kind' => 'aggregate',
                    'relation' => $name,
                    'function' => $function,
                    'column' => $column,
                    'parents' => $idCount,
                    'hits' => count($cachedValues),
                    'misses' => count($missed),
                ]
            );

            foreach ($models as $model) {
                $id = $model->getKey();
                $value = $cachedValues[$id] ?? $fetched[$id] ?? null;
                $hydrator($model, $alias, $value);
            }

            $offset += $idCount;
        }

        if (!empty($toCache)) {
            NormCache::setRelationAggregates($toCache);
        }

        return $models;
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function fetchMissed(
        array $missedIds,
        string $name,
        ?callable $constraint,
        string $function,
        string $column,
        string $pkName,
        string $alias,
    ): array {
        $relations = $constraint !== null ? [$name => $constraint] : $name;

        $query = ($this->model::class)::withoutCache()
            ->withoutGlobalScope(SoftDeletingScope::class)
            ->select($pkName)
            ->withAggregate($relations, $column, $function)
            ->whereIn($pkName, $missedIds);

        if ($this->parentBuilder?->hasRemovedScopes()) {
            $this->parentBuilder->applyRemovedScopesTo($query);
        }

        return $query->get()->pluck($alias, $pkName)->all();
    }

    private function constraintKey(string $relatedClass, ?callable $constraint): string
    {
        $builder = (new $relatedClass)->newQuery()->withoutCache();

        if ($constraint !== null) {
            $constraint($builder);
        }

        return QueryHasher::fromQuery($builder->toBase());
    }
}
