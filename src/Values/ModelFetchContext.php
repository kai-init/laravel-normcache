<?php

namespace NormCache\Values;

use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;

final class ModelFetchContext
{
    /** @var array<int|string, Model> id => hydrated model */
    public array $hits = [];

    public ?string $lockKey = null;

    public ?string $wakeKey = null;

    public ?string $token = null;

    public float $databaseTimeMs = 0.0;

    public float $redisTimeMs = 0.0;

    public float $hydrationTimeMs = 0.0;

    public function __construct(
        public readonly string $modelClass,
        public readonly string $classKey,
        public readonly ?array $projection,
        public readonly ?Model $prototype,
        public readonly ?CacheableBuilder $missedQuery,
        public readonly bool $preserveQueryShape,
        public int $modelVersion,
    ) {}
}
