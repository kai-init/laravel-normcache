<?php

namespace NormCache\Planning;

use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
use NormCache\Values\CachePlanContext;
use NormCache\Values\DependencySet;

final class DependencyResolver
{
    public function resolve(
        string $modelClass,
        CachePlanContext $context,
        QueryInspection $inspection,
        ?array $explicitModels,
        array $explicitTables,
        bool $hasExplicit,
    ): DependencySet {
        $inferred = $inspection->dependencies;
        $required = $context->requiredDependencies;

        if ($hasExplicit) {
            return new DependencySet(
                models: array_values(array_unique([
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
                // An explicit dependsOn() adds deps, it doesn't vouch for ones QueryAnalyzer couldn't infer.
                safe: $inferred->safe && $required->safe,
                reasons: [...$inferred->reasons, ...$required->reasons],
            );
        }

        if ($inspection->hasDependencyBypass()
            || isset($inspection->contextReasons['dependency'])
            || !$inferred->safe
            || !$required->safe) {
            return DependencySet::unsafe(array_values(array_unique([
                ...(BypassReasons::fromInspection($inspection)['dependency'] ?? []),
                ...($inspection->contextReasons['dependency'] ?? []),
                ...$inferred->reasons,
                ...$required->reasons,
            ])));
        }

        if ($inferred->hasNoDependencies() && $required->hasNoDependencies()) {
            return DependencySet::singleModel($modelClass);
        }

        return new DependencySet(
            models: array_values(array_unique([
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

    public function resolveBase(
        CacheableBuilder $builder,
        Model $model,
        CachePlanContext $context,
    ): DependencySet {
        $required = $context->requiredDependencies;

        return new DependencySet(
            models: array_values(array_unique([
                $model::class,
                ...$required->models,
                ...($builder->explicitDependencies() ?? []),
            ])),
            tables: array_values(array_unique([
                ...$required->tables,
                ...$builder->explicitTableDependencies(),
            ])),
            safe: $required->safe,
            reasons: $required->reasons,
        );
    }
}
