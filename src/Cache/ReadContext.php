<?php

namespace NormCache\Cache;

use NormCache\Database\QueryBuilder;
use NormCache\Values\QueryPlan;

final readonly class ReadContext
{
    public function __construct(
        public QueryBuilder $query,
        public QueryPlan $plan,
        public string $namespace,
    ) {}
}
