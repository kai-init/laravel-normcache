<?php

namespace NormCache\Values;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;

/** Per-call state for ModelHydrator::getModels(); mutable so the miss path can accumulate hits and hold the build lock. */
final class ModelFetchContext
{
    /** @var array<int|string, Model> id => hydrated model */
    public array $hits = [];

    public ?string $lockKey = null;

    public ?string $wakeKey = null;

    public ?string $token = null;

    public function __construct(
        public readonly string $modelClass,
        public readonly string $classKey,
        public readonly ?array $projection,
        public readonly ?Model $prototype,
        public readonly ?EloquentBuilder $missedQuery,
        public readonly bool $preserveQueryShape,
        public int $modelVersion,
    ) {}
}
