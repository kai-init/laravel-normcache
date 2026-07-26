<?php

namespace NormCache\Database\Connections;

use NormCache\Database\CachingQueryBuilder;

use function Illuminate\Support\enum_value;

trait BuildsCachingQueries
{
    public function query(): CachingQueryBuilder
    {
        return new CachingQueryBuilder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor(),
        );
    }

    public function table($table, $as = null): CachingQueryBuilder
    {
        return $this->query()
            ->from(enum_value($table), $as)
            ->markDbTable();
    }
}
