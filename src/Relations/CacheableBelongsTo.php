<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\BelongsTo;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;

class CacheableBelongsTo extends BelongsTo
{
    private array $eagerKeys = [];

    public function addEagerConstraints(array $models): void
    {
        parent::addEagerConstraints($models);

        $this->eagerKeys = $this->getEagerModelKeys($models);
    }

    public function getEager()
    {
        if (!$this->shouldUseCacheForEagerLoad()) {
            return parent::getEager();
        }

        return $this->related->newCollection(
            NormCache::getModels($this->eagerKeys, $this->related::class)
        );
    }

    private function shouldUseCacheForEagerLoad(): bool
    {
        if ($this->eagerKeys === []
            || !NormCache::isEnabled()
            || !$this->query instanceof CacheableBuilder
            || $this->query->isCacheSkipped()
            || $this->parent->getConnection()->transactionLevel() > 0
            || $this->getOwnerKeyName() !== $this->related->getKeyName()
            || $this->query->getEagerLoads() !== []) {
            return false;
        }

        $base = $this->query->toBase();

        if (count($base->wheres ?? []) !== 1
            || !empty($base->orders)
            || $base->columns !== null
            || $base->offset > 0
            || $base->limit > 0) {
            return false;
        }

        $where = $base->wheres[0];

        if (($where['column'] ?? null) !== $this->getQualifiedOwnerKeyName()) {
            return false;
        }

        return ($where['type'] ?? null) === 'In' || ($where['type'] ?? null) === 'InRaw';
    }
}
