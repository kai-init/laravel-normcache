<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Illuminate\Support\Str;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

class AggregateLoader
{
    protected static array $cache = [];

    public function __construct(private Model $model) {}

    public function load(array $models, array $pendingAggregates): array
    {
        if (empty($models)) {
            return $models;
        }

        $parentClass = $this->model::class;
        $pkName = $this->model->getKeyName();
        $prefix = "agg:{" . NormCache::classKey($parentClass) . "}:";

        $ids = array_map(fn($m) => $m->getKey(), $models);
        $idCount = count($ids);

        $specs = [];
        $keys = [];
        $offset = 0;

        foreach ($pendingAggregates as $agg) {
            ['name' => $name, 'constraint' => $constraint, 'function' => $function, 'column' => $column] = $agg;

            $relatedClass = $this->resolveRelatedClass($parentClass, $name);
            $alias = $this->resolveAlias($name, $function, $column);
            $constraintHash = $this->constraintKey($relatedClass, $constraint);
            $version = NormCache::currentVersion($relatedClass);
            $suffix = ":{$column}:{$function}:{$name}:{$constraintHash}:v{$version}";

            foreach ($ids as $id) {
                $keys[] = "{$prefix}{$id}{$suffix}";
            }

            $specs[] = ['agg' => $agg, 'alias' => $alias, 'suffix' => $suffix, 'offset' => $offset];
            $offset += $idCount;
        }

        $data = NormCache::getMany($keys);
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

            foreach ($ids as $j => $id) {
                $v = $data[$offset + $j];
                if (is_array($v)) {
                    $cachedValues[$id] = $v['v'];
                } else {
                    $missed[] = $id;
                }
            }

            $fetched = [];
            if (!empty($missed)) {
                $fetched = $this->fetchMissed($missed, $name, $constraint, $function, $column, $pkName, $alias);

                foreach ($missed as $id) {
                    $toCache["{$prefix}{$id}{$suffix}"] = ['v' => $fetched[$id] ?? null];
                }
            }

            foreach ($models as $model) {
                $id = $model->getKey();
                $value = $cachedValues[$id] ?? $fetched[$id] ?? null;
                $hydrator($model, $alias, $value);
            }
        }

        if (!empty($toCache)) {
            NormCache::setMany($toCache, NormCache::queryTtl());
        }

        return $models;
    }

    private function resolveRelatedClass(string $parentClass, string $name): string
    {
        return self::$cache["rc:{$parentClass}:{$name}"] ??= $this->model->{$name}()->getRelated()::class;
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
        if ($constraint === null) {
            return 'nc';
        }

        $builder = (new $relatedClass)->newQueryWithoutScopes()->withoutCache();
        $constraint($builder);

        return QueryHasher::fromQuery($builder->toBase());
    }

    private function resolveAlias(string $name, string $function, string $column): string
    {
        return self::$cache["al:{$name}:{$function}:{$column}"] ??= Str::snake(preg_replace('/[^[:alnum:][:space:]_]/u', '', "$name $column $function"));
    }
}
