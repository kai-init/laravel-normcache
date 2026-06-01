<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Relations\HasOneOrManyThrough;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;
use NormCache\Support\QueryHasher;
use NormCache\Support\QueryInspector;

trait CachesRelationAggregates
{
    private bool $cacheAggregates = true;

    private array $pendingAggregates = [];

    public function withoutAggregateCache(): static
    {
        $this->cacheAggregates = false;

        if (!empty($this->pendingAggregates)) {
            $this->replayPendingAggregates();
        }

        return $this;
    }

    public function withAggregate($relations, $column, $function = null): static
    {
        if (!$this->cacheAggregates) {
            return parent::withAggregate($relations, $column, $function);
        }

        $uncacheable = [];

        // mirrors Eloquent's alias formula exactly, including Expression unwrapping.
        $lowerFunction = strtolower((string) $function);
        $colValue = $this->getQuery()->getGrammar()->isExpression($column)
            ? $this->getQuery()->getGrammar()->getValue($column)
            : $column;

        foreach (Arr::wrap($relations) as $name => $constraint) {
            if (is_numeric($name)) {
                $name = $constraint;
                $constraint = null;
            }

            $explicitAlias = null;
            $segments = explode(' ', (string) $name);
            if (count($segments) === 3 && Str::lower($segments[1]) === 'as') {
                [$name, $explicitAlias] = [$segments[0], $segments[2]];
            }

            $alias = $explicitAlias ?? Str::snake(
                preg_replace('/[^[:alnum:][:space:]_]/u', '', "{$name} {$lowerFunction} {$colValue}")
            );

            $original = $explicitAlias !== null ? "{$name} as {$explicitAlias}" : $name;

            $entry = $this->classifyAggregate($name, $alias, $constraint, $lowerFunction, $colValue);

            if ($entry === null) {
                if ($constraint !== null) {
                    $uncacheable[$original] = $constraint;
                } else {
                    $uncacheable[] = $original;
                }

                continue;
            }

            $this->pendingAggregates[] = $entry;
        }

        if (!empty($uncacheable)) {
            parent::withAggregate($uncacheable, $column, $function);
        }

        return $this;
    }

    private function classifyAggregate(
        string $name,
        string $alias,
        ?callable $constraint,
        string $function,
        string $column,
    ): ?array {
        if (str_contains($name, '.')) {
            return null;
        }

        $relation = $this->model->{$name}();
        $relatedClass = $relation->getRelated()::class;

        if (!self::relatedIsCacheable($relatedClass)) {
            return null;
        }

        // Constraints using relation-specific APIs or cross-table reads can't have deps inferred.
        if ($constraint !== null) {
            try {
                $testBuilder = ($relatedClass)::withoutCache();
                $constraint($testBuilder);
                if (QueryInspector::hasDependencyBypass($testBuilder->toBase())) {
                    return null;
                }
            } catch (\Throwable) {
                return null;
            }
        }

        // Through model FK changes must invalidate the blob; skip non-Cacheable through models.
        $throughClass = null;
        if ($relation instanceof HasOneOrManyThrough) {
            $through = (new \ReflectionProperty($relation, 'throughParent'))->getValue($relation)::class;
            if (self::relatedIsCacheable($through)) {
                $throughClass = $through;
            }
        }

        return [
            'name' => $name,
            'alias' => $alias,
            'constraint' => $constraint,
            'function' => $function,
            'column' => $column,
            'relatedClass' => $relatedClass,
            'throughClass' => $throughClass,
        ];
    }

    private function getFromAggregateCache(QueryBuilder $base, string $model, mixed $columns): Collection
    {
        $debugbarStart = NormCacheCollector::beginMeasure();
        $depClasses = array_values(array_unique(array_merge(
            $this->dependsOn ?? [],
            $this->inferAggregateDependencies(),
        )));
        $hash = $this->aggregateHash($base);

        $result = NormCache::getRawCache($model, $depClasses, $hash, $this->cacheTag);

        if ($result['status'] === 'building') {
            $result = NormCache::waitForBuild($model, $hash, depClasses: $depClasses, tag: $this->cacheTag);

            if ($result === null) {
                if (NormCache::isEventsEnabled()) {
                    event(new QueryCacheMiss($model, 'building:budget-exhausted'));
                }

                NormCacheCollector::recordQuery('query miss', $model, 'building:budget-exhausted', $debugbarStart, ['kind' => 'agg-blob']);

                $this->replayPendingAggregates();

                return $this->finalizeResult(parent::get($columns)->all());
            }
        }

        if ($result['status'] === 'miss') {
            if (NormCache::isEventsEnabled()) {
                event(new QueryCacheMiss($model, $result['key']));
            }

            NormCacheCollector::recordQuery('query miss', $model, $result['key'], $debugbarStart, ['kind' => 'agg-blob']);

            $aggAliases = array_column($this->pendingAggregates, 'alias');
            $this->replayPendingAggregates();
            $models = parent::get($columns);
            // getRawOriginal() preserves $hidden; getAttribute(alias) applies dynamic casts (withExists -> bool).
            $blob = $models->map(function ($m) use ($aggAliases) {
                $attrs = $m->getRawOriginal();
                foreach ($aggAliases as $alias) {
                    $attrs[$alias] = $m->getAttribute($alias);
                }

                return $attrs;
            })->all();
            NormCache::storeRawResult($result['key'], $blob, $result['buildingKey'], $this->queryTtl, $result['wakeKey'] ?? null, $result['versionKeys'], $result['expectedVersions']);

            return $this->finalizeResult($models->all());
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($model, $result['key']));
        }

        NormCacheCollector::recordQuery('query hit', $model, $result['key'], $debugbarStart, [
            'kind' => 'agg-blob',
            'contains' => class_basename($model) . ' (' . count($result['blob']) . ' models)',
        ]);

        $this->pendingAggregates = [];

        return $this->finalizeResult(NormCache::hydrateRaw($result['blob'], $model));
    }

    private function inferAggregateDependencies(): array
    {
        $classes = [];
        foreach ($this->pendingAggregates as $agg) {
            $classes[] = $agg['relatedClass'];
            if ($agg['throughClass'] ?? null) {
                $classes[] = $agg['throughClass'];
            }
        }

        return array_values(array_unique($classes));
    }

    private static function relatedIsCacheable(string $class): bool
    {
        static $cache = [];

        return $cache[$class] ??= in_array(Cacheable::class, class_uses_recursive($class), true);
    }

    private function aggregateHash(QueryBuilder $base): string
    {
        $baseHash = $this->rawCacheKey($base);
        $specHashes = array_map(function ($agg) {
            $builder = ($agg['relatedClass'])::withoutCache();
            if ($agg['constraint'] !== null) {
                ($agg['constraint'])($builder);
            }

            return $agg['name'] . ':' . $agg['alias'] . ':' . $agg['function'] . ':' . $agg['column'] . ':' . QueryHasher::fromQuery($builder->toBase());
        }, $this->pendingAggregates);

        return sha1($baseHash . ':' . implode(':', $specHashes));
    }

    private function replayPendingAggregates(): void
    {
        foreach ($this->pendingAggregates as $agg) {
            $name = "{$agg['name']} as {$agg['alias']}";
            $relations = $agg['constraint'] !== null ? [$name => $agg['constraint']] : $name;

            parent::withAggregate($relations, $agg['column'], $agg['function']);
        }

        $this->pendingAggregates = [];
    }

    private function queryReferencesPendingAggregateAlias(QueryBuilder $base): bool
    {
        if (empty($this->pendingAggregates)) {
            return false;
        }

        $aliases = array_column($this->pendingAggregates, 'alias');

        foreach ($base->orders ?? [] as $clause) {
            if (isset($clause['column']) && in_array($clause['column'], $aliases, true)) {
                return true;
            }
            if (isset($clause['sql']) && $this->rawSqlReferencesAlias((string) $clause['sql'], $aliases)) {
                return true;
            }
        }

        foreach ($base->havings ?? [] as $clause) {
            if (isset($clause['column']) && in_array($clause['column'], $aliases, true)) {
                return true;
            }
            if (isset($clause['sql']) && $this->rawSqlReferencesAlias((string) $clause['sql'], $aliases)) {
                return true;
            }
        }

        foreach ($base->groups ?? [] as $group) {
            if (is_string($group) && in_array($group, $aliases, true)) {
                return true;
            }
        }

        foreach ($base->wheres as $clause) {
            if (isset($clause['column']) && in_array($clause['column'], $aliases, true)) {
                return true;
            }
            if (isset($clause['sql']) && $this->rawSqlReferencesAlias((string) $clause['sql'], $aliases)) {
                return true;
            }
        }

        return false;
    }

    private function rawSqlReferencesAlias(string $sql, array $aliases): bool
    {
        foreach ($aliases as $alias) {
            if (preg_match('/(?<![A-Za-z0-9_])' . preg_quote($alias, '/') . '(?![A-Za-z0-9_])/', $sql)) {
                return true;
            }
        }

        return false;
    }
}
