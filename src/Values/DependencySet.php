<?php

namespace NormCache\Values;

final readonly class DependencySet
{
    public function __construct(
        public array $models = [],
        public array $tables = [],
        public bool $safe = true,
        public array $reasons = [],
    ) {}

    public static function unsafe(string|array $reasons): self
    {
        return new self(safe: false, reasons: is_array($reasons) ? array_values($reasons) : [$reasons]);
    }

    public function merge(self $plan): self
    {
        return new self(
            models: array_values(array_unique([...$this->models, ...$plan->models])),
            tables: array_values(array_unique([...$this->tables, ...$plan->tables])),
            safe: $this->safe && $plan->safe,
            reasons: [...$this->reasons, ...$plan->reasons],
        );
    }

    public function depClassesFor(string $primaryModel): array
    {
        return array_values(array_filter(
            $this->models,
            fn(string $model): bool => $model !== $primaryModel,
        ));
    }
}
