<?php

namespace NormCache\Values;

final readonly class AggregateDependencyResult
{
    public function __construct(
        public DependencySet $dependencies,
        public array $aliases,
        public bool $safe,
        public ?string $unsafeReason = null,
    ) {}
}
