<?php

namespace NormCache\Planning;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder as QueryBuilder;
use Illuminate\Support\Facades\Log;
use NormCache\CacheableBuilder;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;
use NormCache\Values\QueryInspection;

/**
 * Resolves the models/tables a cached query depends on, and (in debug) warns on
 * tables the caller failed to declare. Split from CachePlanner for isolated
 * testing and as the seam cache-spaces validates dependencies against.
 */
final class DependencyResolver
{
    public function __construct(
        private readonly QueryAnalyzer $analyzer = new QueryAnalyzer,
    ) {}

    public function resolve(
        string $modelClass,
        CachePlanContext $context,
        ?QueryInspection $inspection,
        ?array $explicitModels,
        array $explicitTables,
        bool $hasExplicit,
    ): DependencySet {
        $inferred = $context->inferredDependencies;
        $required = $context->requiredDependencies;

        if ($hasExplicit) {
            return new DependencySet(
                models: array_keys(array_flip([
                    $modelClass,
                    ...$inferred->models,
                    ...$required->models,
                    ...($explicitModels ?? []),
                ])),
                tables: array_values(array_unique([
                    ...$inferred->tables,
                    ...$required->tables,
                    ...$explicitTables,
                ])),
                safe: $required->safe,
                reasons: $required->reasons,
            );
        }

        $hasDependencyBypass = $inspection !== null && $inspection->hasDependencyBypass();

        // EXISTS_WHERE-only bypasses are exempt if inferred dependencies are safe and non-empty.
        $exempt = $hasDependencyBypass
            && $inspection->hasOnlyExistsDependencyBypass()
            && $inferred->safe
            && !$inferred->hasNoDependencies();

        if (($hasDependencyBypass && !$exempt)
            || isset($context->contextReasons['dependency'])
            || !$inferred->safe
            || !$required->safe) {
            return DependencySet::unsafe(array_values(array_unique([
                ...($inspection !== null ? BypassReasons::fromInspection($inspection)['dependency'] ?? [] : []),
                ...($context->contextReasons['dependency'] ?? []),
                ...$inferred->reasons,
                ...$required->reasons,
            ])));
        }

        if ($inferred->hasNoDependencies() && $required->hasNoDependencies()) {
            return DependencySet::singleModel($modelClass);
        }

        return new DependencySet(
            models: array_keys(array_flip([
                $modelClass,
                ...$inferred->models,
                ...$required->models,
            ])),
            tables: array_values(array_unique([
                ...$inferred->tables,
                ...$required->tables,
            ])),
        );
    }

    // Dependencies for plans made without query inspection (global/transaction bypasses).
    public function resolveBase(
        CacheableBuilder $builder,
        Model $model,
        CachePlanContext $context,
    ): DependencySet {
        return $this->resolve(
            $model::class,
            $context,
            null,
            $builder->explicitDependencies(),
            $builder->explicitTableDependencies(),
            $builder->hasExplicitDependencies(),
        );
    }

    public function warnUnderDeclared(
        string $modelTable,
        QueryBuilder $base,
        QueryInspection $inspection,
        DependencySet $dependencies,
    ): void {
        if (!config('app.debug', false)) {
            return;
        }

        if ($inspection->has(QueryInspection::EXISTS_WHERE | QueryInspection::SUBQUERY_WHERE)) {
            Log::warning(
                'NormCache Warning: Query contains subquery/exists predicates. NormCache cannot verify all touched tables; ensure dependsOn()/dependsOnTables() includes every table read by the subquery.'
            );
        }

        $this->checkDependencyCompleteness(
            $inspection->tables ?? $this->analyzer->extractTables($base, $modelTable),
            $dependencies,
            $modelTable,
        );
    }

    private function checkDependencyCompleteness(array $queryTables, DependencySet $dependencies, string $baseTable): void
    {
        // Strip connection prefix from table keys ("conn:table" → "table").
        $declaredTables = array_map(
            fn($key) => str_contains($key, ':') ? substr($key, strpos($key, ':') + 1) : $key,
            $dependencies->tables
        );

        // Map declared models to their tables.
        foreach ($dependencies->models as $modelClass) {
            if (class_exists($modelClass) && is_subclass_of($modelClass, Model::class)) {
                $declaredTables[] = (new $modelClass)->getTable();
            }
        }

        // Add the base table to the declared list so it doesn't get flagged as missing.
        $declaredTables[] = $baseTable;

        $missing = array_diff($queryTables, $declaredTables);

        if (!empty($missing)) {
            $tablesStr = implode(', ', $missing);
            Log::warning(
                "NormCache Warning: Query touches tables ({$tablesStr}) that are not present in dependsOn()/dependsOnTables(). This is an under-declared dependency and can lead to outdated cache reads."
            );
        }
    }
}
