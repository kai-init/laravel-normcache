<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletingScope;
use Illuminate\Support\Str;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;

class AggregateLoader
{
    public function __construct(private Model $model) {}

    public function load(array $models, array $pendingAggregates): array
    {
        if (empty($models)) {
            return $models;
        }

        $parentClass = $this->model::class;
        $pkName = $this->model->getKeyName();
        $classKey = NormCache::classKey($parentClass);

        foreach ($pendingAggregates as $agg) {
            ['name' => $name, 'constraint' => $constraint, 'function' => $function, 'column' => $column] = $agg;

            $relation = $this->model->{$name}();
            $relatedModel = $relation->getRelated();
            $relatedClass = $relatedModel::class;
            $constraintHash = $this->constraintKey($relatedModel, $constraint);
            $alias = $this->resolveAlias($name, $function, $column);

            $ids = array_map(fn($m) => $m->getKey(), $models);
            $cache = NormCache::getAggregateCache($parentClass, $relatedClass, $ids, $column, $function, $name, $constraintHash);
            $relatedVersion = $cache['version'];
            $cachedById = $cache['data'];

            $missed = array_keys(array_filter($cachedById, fn($v) => !is_array($v)));

            $fromDb = [];
            if (!empty($missed)) {
                $fromDb = $this->fetchMissed($missed, $name, $constraint, $function, $column, $pkName, $alias);

                $toCache = [];
                foreach ($missed as $id) {
                    $key = "agg:{$classKey}:{$id}:{$column}:{$function}:{$name}:{$constraintHash}:v{$relatedVersion}";
                    $toCache[$key] = ['v' => $fromDb[$id] ?? null];
                }

                NormCache::setMany($toCache, NormCache::queryTtl());
            }

            foreach ($models as $model) {
                $id = $model->getKey();
                $value = is_array($cachedById[$id]) ? $cachedById[$id]['v'] : ($fromDb[$id] ?? null);
                $model->setAttribute($alias, $value);
            }
        }

        return $models;
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

    private function constraintKey(Model $relatedModel, ?callable $constraint): string
    {
        if ($constraint === null) {
            return 'nc';
        }

        $builder = $relatedModel->newQueryWithoutScopes()->withoutCache();
        $constraint($builder);
        $base = $builder->toBase();

        return QueryHasher::hash($base);
    }

    private function resolveAlias(string $name, string $function, string $column): string
    {
        return Str::snake(preg_replace('/[^[:alnum:][:space:]_]/u', '', "$name $column $function"));
    }
}
