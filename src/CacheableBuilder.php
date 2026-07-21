<?php

namespace NormCache;

use Closure;
use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation as EloquentRelation;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Arr;
use NormCache\Cache\ModelsExecutor;
use NormCache\Enums\CacheStrategy;
use NormCache\Enums\PlanningMode;
use NormCache\Enums\ResultKind;
use NormCache\Facades\NormCache;
use NormCache\Planning\BypassReasons;
use NormCache\Planning\CachePlanner;
use NormCache\Relations\CachesRelationAggregates;
use NormCache\Relations\CachesRelationExistence;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\ProjectionClassifier;
use NormCache\Traits\Cacheable;
use NormCache\Traits\CachesScalarResults;
use NormCache\Traits\HandlesBuilderInvalidation;
use NormCache\Values\CachePlan;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\PreparedQuery;
use NormCache\Values\QueryInspection;

class CacheableBuilder extends Builder
{
    use CachesRelationAggregates, CachesRelationExistence, CachesScalarResults, HandlesBuilderInvalidation;

    private static array $validatedModelClasses = [];

    private bool $skipCache = false;

    private ?int $queryTtl = null;

    private ?array $dependsOn = null;

    private array $dependsOnTables = [];

    private ?string $cacheTag = null;

    private ?string $cacheSpace = null;

    private ?DependencySet $capturedDependencies = null;

    private array $capturedContextReasons = [];

    private int $capturedOpaqueJoins = 0;

    private bool $capturedOpaqueFrom = false;

    private int $capturedOpaqueWhereSubqueries = 0;

    // -------------------------------------------------------------------------
    // Configuration
    // -------------------------------------------------------------------------

    public function withoutCache(): static
    {
        $this->skipCache = true;

        return $this;
    }

    public function isCacheSkipped(): bool
    {
        return $this->skipCache;
    }

    public function ttl(int $ttl): static
    {
        if ($ttl <= 0) {
            throw new \InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->queryTtl = $ttl;

        return $this;
    }

    public function tag(string $tag): static
    {
        CacheKeyBuilder::assertValidTag($tag);

        $this->cacheTag = $tag;

        return $this;
    }

    public function space(string $name): static
    {
        $this->cacheSpace = $name;

        return $this;
    }

    public function getSpace(): ?string
    {
        return $this->cacheSpace;
    }

    public function dependsOn(array $modelClasses): static
    {
        if (empty($modelClasses)) {
            throw new \InvalidArgumentException('dependsOn() requires at least one model class.');
        }

        $existing = $this->dependsOn ?? [];

        foreach ($modelClasses as $class) {
            if (!is_string($class)) {
                throw new \InvalidArgumentException('dependsOn() expects model class names, not model instances.');
            }

            if (!isset(self::$validatedModelClasses[$class])) {
                if (!is_a($class, Model::class, true)) {
                    throw new \InvalidArgumentException("dependsOn() class [{$class}] must be an Eloquent model.");
                }

                if (!in_array(Cacheable::class, class_uses_recursive($class), true)) {
                    throw new \InvalidArgumentException(
                        "dependsOn() class [{$class}] must use the NormCache\\Cacheable trait."
                    );
                }

                self::$validatedModelClasses[$class] = true;
            }

            if (!in_array($class, $existing, true)) {
                $existing[] = $class;
            }
        }

        $this->dependsOn = $existing;

        return $this;
    }

    public function dependsOnTables(array $tables): static
    {
        if (empty($tables)) {
            throw new \InvalidArgumentException('dependsOnTables() requires at least one table name.');
        }

        foreach ($tables as $table) {
            if (!is_string($table) || $table === '') {
                throw new \InvalidArgumentException('dependsOnTables() expects non-empty table name strings.');
            }

            if (preg_match('/[:{}\s*]/', $table)) {
                throw new \InvalidArgumentException(
                    'dependsOnTables() table name must not contain reserved characters (: { } * or whitespace).'
                );
            }
        }

        $conn = $this->model->getConnection()->getName();
        $this->dependsOnTables = array_values(array_unique(array_merge(
            $this->dependsOnTables,
            array_map(fn($table) => NormCache::keys()->tableKey($conn, $table), $tables),
        )));

        return $this;
    }

    public function explicitDependencies(): ?array
    {
        return $this->dependsOn;
    }

    public function explicitTableDependencies(): array
    {
        return $this->dependsOnTables;
    }

    public function hasExplicitDependencies(): bool
    {
        return $this->dependsOn !== null || $this->dependsOnTables !== [];
    }

    public function capturedDependencies(): DependencySet
    {
        return $this->capturedDependencies ?? DependencySet::empty();
    }

    public function capturedContextReasons(): array
    {
        return $this->capturedContextReasons;
    }

    public function capturedOpaqueJoins(): int
    {
        return $this->capturedOpaqueJoins;
    }

    public function hasCapturedOpaqueFrom(): bool
    {
        return $this->capturedOpaqueFrom;
    }

    public function capturedOpaqueWhereSubqueries(): int
    {
        return $this->capturedOpaqueWhereSubqueries;
    }

    public function addCapturedContextReason(string $category, string $reason): void
    {
        $this->capturedContextReasons[$category] = array_values(array_unique([
            ...($this->capturedContextReasons[$category] ?? []),
            $reason,
        ]));
    }

    /**
     * @param  Closure(CacheableBuilder): mixed|array|string|Expression  $column
     */
    public function where($column, $operator = null, $value = null, $boolean = 'and'): static
    {
        if (!$column instanceof Closure || $operator !== null) {
            return parent::where($column, $operator, $value, $boolean);
        }

        $nested = $this->model->newQueryWithoutRelationships();

        if (!$nested instanceof self) {
            return parent::where($column, $operator, $value, $boolean);
        }

        $column($nested);

        $this->mergeCapturedBuilderState($nested);
        $this->eagerLoad = array_merge($this->eagerLoad, $nested->getEagerLoads());
        $this->withoutGlobalScopes($nested->removedScopes());
        $this->query->addNestedWhereQuery($nested->getQuery(), $boolean);

        return $this;
    }

    public function selectSub($query, $as): static
    {
        $this->captureSubqueryDependencies($query);
        $this->query->selectSub($query, $as);

        return $this;
    }

    public function joinSub($query, $as, $first, $operator = null, $second = null, $type = 'inner', $where = false): static
    {
        $this->captureSubqueryDependencies($query);
        $this->capturedOpaqueJoins++;
        $this->query->joinSub($query, $as, $first, $operator, $second, $type, $where);

        return $this;
    }

    public function fromSub($query, $as): static
    {
        $this->captureSubqueryDependencies($query);
        $this->capturedOpaqueFrom = true;
        $this->query->fromSub($query, $as);

        return $this;
    }

    public function whereIn($column, $values, $boolean = 'and', $not = false): static
    {
        if ($values instanceof Closure) {
            $callback = $values;
            $callback($values = $this->query->newQuery());
        }

        if ($values instanceof Builder || $values instanceof QueryBuilder || $values instanceof EloquentRelation) {
            $this->captureSubqueryDependencies($values);
            $this->capturedOpaqueWhereSubqueries++;
        }

        $this->query->whereIn($column, $values, $boolean, $not);

        return $this;
    }

    public function whereNotIn($column, $values, $boolean = 'and'): static
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    public function orWhereIn($column, $values): static
    {
        return $this->whereIn($column, $values, 'or');
    }

    public function orWhereNotIn($column, $values): static
    {
        return $this->whereIn($column, $values, 'or', true);
    }

    private function captureSubqueryDependencies(mixed $query): void
    {
        $base = match (true) {
            $query instanceof self => $query->toBase(),
            $query instanceof Builder => $query->toBase(),
            $query instanceof QueryBuilder => $query,
            $query instanceof EloquentRelation => $query->getQuery()->toBase(),
            default => null,
        };

        if ($base === null) {
            $this->addCapturedContextReason('dependency', 'subquery dependency could not be inferred');

            return;
        }

        $connection = $this->model->getConnection()->getName();
        $analyzer = $this->planner()->analyzer();
        $dependencies = $analyzer->inferQueryDependencies($base, $connection);
        $this->capturedDependencies = ($this->capturedDependencies ?? DependencySet::empty())->merge($dependencies);

        $table = is_string($base->from) ? CacheKeyBuilder::stripTableAlias($base->from) : $this->model->getTable();
        $reasons = BypassReasons::fromInspection(new QueryInspection(
            flags: $analyzer->flags($base, $table, $base->columns),
        ));

        foreach ($reasons as $category => $items) {
            foreach ($items as $reason) {
                $this->addCapturedContextReason($category, $reason);
            }
        }
    }

    protected function mergeCapturedBuilderState(self $builder): void
    {
        $this->capturedDependencies = ($this->capturedDependencies ?? DependencySet::empty())
            ->merge($builder->capturedDependencies());

        foreach ($builder->capturedContextReasons() as $category => $reasons) {
            foreach ($reasons as $reason) {
                $this->addCapturedContextReason($category, $reason);
            }
        }

        $this->capturedOpaqueJoins += $builder->capturedOpaqueJoins();
        $this->capturedOpaqueFrom = $this->capturedOpaqueFrom || $builder->hasCapturedOpaqueFrom();
        $this->capturedOpaqueWhereSubqueries += $builder->capturedOpaqueWhereSubqueries();
    }

    public function getQueryTtl(): ?int
    {
        return $this->queryTtl;
    }

    public function getCacheTag(): ?string
    {
        return $this->cacheTag;
    }

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    public function explain(): string
    {
        $prepared = $this->prepareCacheExecution();
        $plan = $this->planPrepared($prepared, fn() => CachePlanContext::models(
            ProjectionClassifier::resolve($prepared->base, ['*']),
            selectAll: true,
        ), PlanningMode::Explain);

        $space = ($plan->space !== null && $plan->space->name !== 'default')
            ? ' [space: ' . $plan->space->name . ']'
            : '';

        return match ($plan->strategy) {
            CacheStrategy::DirectModels => 'cached: direct (primary key)' . $space,
            CacheStrategy::NormalizedQuery => 'cached' . $space,
            CacheStrategy::VersionedResult => $this->explainResultStrategy() . $space,
            CacheStrategy::LiveQuery => $this->explainBypassStrategy($plan),
        };
    }

    private function explainResultStrategy(): string
    {
        $hasExplicit = $this->dependsOn !== null || $this->dependsOnTables !== [];

        return $hasExplicit ? 'cached: result (dependsOn())' : 'cached: result';
    }

    private function explainBypassStrategy(CachePlan $plan): string
    {
        $labels = BypassReasons::labels();
        $parts = [];
        foreach ($plan->bypassReasons as $category => $reasons) {
            $parts[] = ($labels[$category] ?? $category) . ': ' . implode(', ', $reasons);
        }

        return 'not cached — ' . implode(' | ', $parts);
    }

    public function get($columns = ['*']): Collection
    {
        if ($this->skipCache || !NormCache::isEnabled()) {
            return parent::get($columns);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $columns = Arr::wrap($columns);
        $prepared = $this->prepareCacheExecution();
        $model = $this->model::class;
        $base = $prepared->base;

        $plan = $this->planPrepared($prepared, fn() => CachePlanContext::models(
            ProjectionClassifier::resolve($base, $columns),
            selectAll: $columns === ['*'],
        ));

        return NormCache::withSpace($plan->space, fn() => match ($plan->strategy) {
            CacheStrategy::DirectModels => CacheFallback::rescue(
                NormCache::config(),
                fn() => $this->modelsExecutor()->runDirect($prepared, $plan->primaryKeys, $model, $plan->columns, $this->model),
                fn() => $prepared->collect($columns),
            ),
            CacheStrategy::NormalizedQuery => CacheFallback::rescue(
                NormCache::config(),
                fn() => $this->modelsExecutor()->runNormalized($prepared, $plan, $model, $plan->columns, $this->cacheTag, $this->queryTtl, $debugbarStart, $this->model),
                fn() => $prepared->collect($columns)
            ),
            CacheStrategy::VersionedResult => $this->executeResultQuery($prepared, $plan, $columns),
            CacheStrategy::LiveQuery => $this->bypassAndReturn($model, $plan->bypassReasons, $debugbarStart, $prepared, $columns),
        });
    }

    private function executeResultQuery(
        PreparedQuery $prepared,
        CachePlan $plan,
        array $columns,
    ): Collection {
        $model = $this->model::class;

        [$payload, $cached] = NormCache::result()->execute(
            $prepared,
            $plan,
            ResultKind::Collection,
            $columns,
            function () use ($prepared, $columns) {
                if ($this->hasAggregateColumns()) {
                    return $this->resultPayloadFromEloquentModels($prepared->collect($columns, false));
                }

                return $prepared->baseWithColumns($columns)->get()->map(fn($row) => (array) $row)->all();
            }
        );

        return $prepared->finalizeModels(NormCache::hydrator()->hydrateResult($payload, $this->model, $cached));
    }

    private function bypassAndReturn(
        string $model,
        array $bypassReasons,
        mixed $debugbarStart,
        PreparedQuery $prepared,
        array $columns,
    ): Collection {
        CacheReporter::queryBypassed($model, $bypassReasons, $debugbarStart);

        return $prepared->collect($columns);
    }

    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null, $total = null): LengthAwarePaginator
    {
        if ($total !== null || $this->skipCache || !NormCache::isEnabled()) {
            return parent::paginate($perPage, $columns, $pageName, $page, $total);
        }

        $debugbarStart = CacheReporter::beginMeasure();

        $prepared = $this->prepareCacheExecution();
        $plan = $this->planPrepared($prepared, fn() => CachePlanContext::paginationCount());

        if ($plan->strategy === CacheStrategy::LiveQuery) {
            CacheReporter::queryBypassed($this->model::class, $plan->bypassReasons, $debugbarStart);

            return parent::paginate($perPage, $columns, $pageName, $page);
        }

        try {
            $cachedTotal = NormCache::withSpace($plan->space, fn() => $this->rememberPaginationTotal($prepared, $plan));
        } catch (\Throwable $e) {
            CacheFallback::fallback(NormCache::config(), $e);

            $cachedTotal = null;
        }

        return parent::paginate($perPage, $columns, $pageName, $page, $cachedTotal);
    }

    public function eagerLoadRelations(array $models): array
    {
        if (!$this->skipCache) {
            return parent::eagerLoadRelations($models);
        }

        $original = $this->eagerLoad;

        try {
            foreach ($this->eagerLoad as $name => $constraint) {
                $this->eagerLoad[$name] = function ($query) use ($constraint) {
                    $constraint($query);
                    $builder = $query instanceof EloquentRelation ? $query->getQuery() : $query;
                    if ($builder instanceof self) {
                        $builder->withoutCache();
                    }
                };
            }

            return parent::eagerLoadRelations($models);
        } finally {
            $this->eagerLoad = $original;
        }
    }

    // -------------------------------------------------------------------------
    // Infrastructure
    // -------------------------------------------------------------------------

    public function prepareCacheExecution(): PreparedQuery
    {
        return $this->prepareScopedQuery()->applyBeforeCallbacks();
    }

    public function prepareScopedQuery(): PreparedQuery
    {
        /** @var self $builder */
        $builder = $this->applyScopes();

        return new PreparedQuery($builder, $builder->getQuery());
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    public function cachePlan(
        QueryBuilder $base,
        CachePlanContext $context,
        PlanningMode $planningMode = PlanningMode::Hot,
    ): CachePlan {
        return $this->planner()->plan($this, $base, $context, $planningMode);
    }

    /** @param  Closure(): CachePlanContext  $context */
    public function planPrepared(
        PreparedQuery $prepared,
        Closure $context,
        PlanningMode $mode = PlanningMode::Hot,
    ): CachePlan {
        $builder = $prepared->builder;
        $base = $prepared->base;

        return $builder->cachePlan($base, $context(), $mode);
    }

    public function planner(): CachePlanner
    {
        return app(CachePlanner::class);
    }

    private function modelsExecutor(): ModelsExecutor
    {
        return app(ModelsExecutor::class);
    }

    private function rememberPaginationTotal(PreparedQuery $prepared, CachePlan $plan): int
    {
        [$value] = NormCache::result()->execute(
            $prepared,
            $plan,
            ResultKind::PaginationCount,
            [],
            fn() => $prepared->base->getCountForPagination()
        );

        return (int) $value;
    }
}
