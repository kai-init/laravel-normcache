<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Illuminate\Support\Str;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

class RelationAggregateLoader
{
    private static array $cache = [];

    public function __construct(private Model $model) {}

    public function load(array $models, array $pendingAggregates): array
    {
        if (empty($models)) {
            return $models;
        }

        $parentClass = $this->model::class;
        $pkName = $this->model->getKeyName();
        $prefix = 'agg:{' . NormCache::classKey($parentClass) . '}:';

        $ids = array_map(fn($m) => $m->getKey(), $models);
        $idCount = count($ids);

        $specs = [];
        $keys = [];
        $offset = 0;

        foreach ($pendingAggregates as $agg) {
            ['name' => $name, 'constraint' => $constraint, 'function' => $function, 'column' => $column] = $agg;

            $relation = $this->model->{$name}();
            $relatedClass = $relation->getRelated()::class;
            $alias = $this->resolveAlias($name, $function, $column);
            $constraintHash = $this->constraintKey($relatedClass, $constraint);
            $suffix = ":{$column}:{$function}:{$name}:{$constraintHash}" . $this->versionSuffix($relation);

            foreach ($ids as $id) {
                $keys[] = "{$prefix}{$id}{$suffix}";
            }

            $specs[] = ['agg' => $agg, 'alias' => $alias, 'suffix' => $suffix, 'offset' => $offset];
            $offset += $idCount;
        }

        $debugbarStart = NormCacheCollector::beginMeasure();
        $data = NormCache::getRelationAggregates($keys);
        $toCache = [];

        $hydrator = self::$cache['hydrator'] ??= \Closure::bind(static function ($model, $key, $value) {
            $model->attributes[$key] = $value;
            $model->original[$key] = $value;
        }, null, Model::class);

        foreach ($specs as $spec) {
            ['agg' => $agg, 'alias' => $alias, 'suffix' => $suffix, 'offset' => $offset] = $spec;
            ['name' => $name, 'constraint' => $constraint, 'function' => $function, 'column' => $column] = $agg;

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
                $fetched = $this->fetchMissed($missed, $name, $constraint, $function, $column, $pkName, $alias);

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
        }

        if (!empty($toCache)) {
            NormCache::setRelationAggregates($toCache);
        }

        return $models;
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function versionSuffix(Relation $relation): string
    {
        $suffix = ':v' . NormCache::currentVersion($relation->getRelated()::class);

        if ($relation instanceof BelongsTo) {
            $suffix .= ':p' . NormCache::currentVersion($relation->getParent()::class);
        }

        if ($relation instanceof HasOneOrManyThrough) {
            $suffix .= ':t' . NormCache::currentVersion($relation->getParent()::class);
        }

        return $suffix;
    }

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

        return ($this->model::class)::withoutCache()
            ->withoutGlobalScope(SoftDeletingScope::class)
            ->select($pkName)
            ->withAggregate($relations, $function, $column)
            ->whereIn($pkName, $missedIds)
            ->get()
            ->pluck($alias, $pkName)
            ->all();
    }

    private function constraintKey(string $relatedClass, ?callable $constraint): string
    {
        $builder = (new $relatedClass)->newQuery()->withoutCache();

        if ($constraint !== null) {
            $constraint($builder);
        }

        return QueryHasher::fromQuery($builder->toBase());
    }

    private function resolveAlias(string $name, string $function, string $column): string
    {
        return self::$cache["al:{$name}:{$function}:{$column}"] ??= Str::snake(preg_replace('/[^[:alnum:][:space:]_]/u', '', "$name $column $function"));
    }
}
