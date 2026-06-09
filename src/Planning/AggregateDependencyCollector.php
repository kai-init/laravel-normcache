<?php

namespace NormCache\Planning;

use NormCache\CacheableBuilder;
use NormCache\Values\AggregateDependencyResult;
use NormCache\Values\DependencySet;

final readonly class AggregateDependencyCollector
{
    public function collect(CacheableBuilder $builder): AggregateDependencyResult
    {
        if (!$builder->isCacheAggregatesEnabled()) {
            if ($builder->hasAggregateInferenceFailed()) {
                $reason = 'Aggregate dependencies could not be inferred.';

                return new AggregateDependencyResult(
                    dependencies: DependencySet::unsafe($reason),
                    aliases: [],
                    safe: false,
                    unsafeReason: $reason,
                );
            }

            return new AggregateDependencyResult(
                dependencies: DependencySet::empty(),
                aliases: [],
                safe: true,
            );
        }

        $dependencies = $builder->getAggregateDependencies();
        $tableDependencies = $builder->getAggregateTableDependencies();
        $aliases = $builder->getAggregateAliases();

        if ($dependencies === [] && $tableDependencies === []) {
            return new AggregateDependencyResult(
                dependencies: DependencySet::empty(),
                aliases: $aliases,
                safe: true,
            );
        }

        return new AggregateDependencyResult(
            dependencies: new DependencySet(
                models: $dependencies,
                tables: $tableDependencies,
            ),
            aliases: $aliases,
            safe: true,
        );
    }
}
