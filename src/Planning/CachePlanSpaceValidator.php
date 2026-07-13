<?php

namespace NormCache\Planning;

use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Values\CachePlan;
use Psr\Log\LoggerInterface;

final class CachePlanSpaceValidator
{
    public static function standalone(): self
    {
        $registry = new CacheSpaceRegistry;

        return new self($registry, new CacheSpaceResolver($registry));
    }

    public function __construct(
        private readonly CacheSpaceRegistry $registry,
        private readonly CacheSpaceResolver $resolver,
        private readonly string $crossSpaceBehavior = 'bypass',
        private readonly bool $debug = false,
        private readonly ?LoggerInterface $logger = null,
    ) {}

    public function validate(
        CachePlan $plan,
        CacheableBuilder $builder,
        Model $model,
        bool $explain = false,
    ): CachePlan {
        if (!$plan->isCacheable()) {
            return $plan;
        }

        $space = $this->resolver->resolve($model::class, $builder->getSpace());

        if ($this->registry->dependenciesAreOnlyModel(
            $model::class,
            $plan->dependencies->models,
            $plan->dependencies->tables,
        )) {
            return $plan->withSpace($space);
        }

        $validation = $this->registry->validateDependencies(
            $space,
            $plan->dependencies->models,
            $plan->dependencies->tables,
            includeDependenciesBySpace: $explain,
        );

        if ($validation->isValid) {
            if (!$explain) {
                $this->registry->registerTableDependencies($space, $plan->dependencies->tables);
            }

            return $plan->withSpace($space);
        }

        $offending = implode(', ', [...$validation->invalidModels, ...$validation->invalidTables]);
        $reason = 'cross-space dependencies for space [' . $space->name . ']: ' . $offending;

        if (!$explain && $this->debug) {
            $modelClass = $model::class;
            $this->logger?->warning(
                "NormCache: query for [{$modelClass}] in space [{$space->name}] depends on [{$offending}] "
                . 'which are not in that space; the query will not cache. Add them to the space or drop the dependency.'
            );
        }

        if (!$explain && $this->crossSpaceBehavior === 'throw') {
            throw new \RuntimeException('NormCache: ' . $reason);
        }

        return CachePlan::bypass(
            operation: $plan->operation,
            dependencies: $plan->dependencies,
            bypassReasons: ['dependency' => [$reason]],
        )->withSpace($space);
    }
}
