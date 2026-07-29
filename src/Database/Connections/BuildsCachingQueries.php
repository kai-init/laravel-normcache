<?php

namespace NormCache\Database\Connections;

use NormCache\Database\QueryBuilder;

use function Illuminate\Support\enum_value;

trait BuildsCachingQueries
{
    public function query(): QueryBuilder
    {
        return new QueryBuilder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor(),
        );
    }

    public function table($table, $as = null): QueryBuilder
    {
        return $this->query()
            ->from(enum_value($table), $as)
            ->enableCachingForTable();
    }
}
