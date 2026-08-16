<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletes;
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

        return $builder->enableCachingForModel(
            $this::class,
            $this->getKeyName(),
            $this->getKeyType(),
            $this->normCacheDeletedAtColumn(),
            $this->normCacheVolatileColumns(),
        );
    }

    // Laravel 12 does not expose Model::isSoftDeletable().
    private function normCacheDeletedAtColumn(): ?string
    {
        if (!isset(class_uses_recursive($this::class)[SoftDeletes::class])) {
            return null;
        }

        return method_exists($this, 'getDeletedAtColumn')
            ? $this->getDeletedAtColumn()
            : null;
    }

    /** @return list<string> */
    private function normCacheVolatileColumns(): array
    {
        if (!property_exists($this, 'volatileColumns')) {
            return [];
        }

        /** @var mixed $declared */
        $declared = $this->volatileColumns;

        return array_values(array_filter(
            is_array($declared) ? $declared : [],
            is_string(...),
        ));
    }
}
