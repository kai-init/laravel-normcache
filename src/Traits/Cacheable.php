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
        $builder = $this->getConnection()->query();

        if (!$builder instanceof QueryBuilder) {
            return $builder;
        }

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
