<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use NormCache\Database\QueryBuilder;

/**
 * @mixin Model
 */
trait Cacheable
{
    protected function newBaseQueryBuilder()
    {
        $connection = $this->getConnection();
        $builder = new QueryBuilder(
            $connection,
            $connection->getQueryGrammar(),
            $connection->getPostProcessor(),
        );

        $deletedAtColumn = method_exists($this, 'getDeletedAtColumn')
            ? $this->getDeletedAtColumn()
            : null;

        return $builder->enableCachingForModel(
            $this::class,
            $this->getKeyName(),
            $this->getKeyType(),
            $deletedAtColumn,
        );
    }
}
