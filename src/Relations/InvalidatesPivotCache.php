<?php

namespace NormCache\Relations;

use NormCache\Facades\NormCache;

trait InvalidatesPivotCache
{
    private bool $syncing = false;

    public function attach($ids, array $attributes = [], $touch = true): void
    {
        parent::attach($ids, $attributes, $touch);

        if (!$this->syncing) {
            $this->recordPivotWrite(true);
        }
    }

    public function detach($ids = null, $touch = true): int
    {
        $affected = parent::detach($ids, $touch);

        if (!$this->syncing) {
            $this->recordPivotWrite($affected > 0);
        }

        return $affected;
    }

    public function updateExistingPivot($id, array $attributes, $touch = true): int
    {
        $affected = parent::updateExistingPivot($id, $attributes, $touch);
        $this->recordPivotWrite($affected > 0);

        return $affected;
    }

    public function sync($ids, $detaching = true): array
    {
        $this->syncing = true;

        try {
            $changes = parent::sync($ids, $detaching);
        } finally {
            $this->syncing = false;
        }

        $this->recordPivotWrite(
            $changes['attached'] !== []
            || $changes['detached'] !== []
            || $changes['updated'] !== [],
        );

        return $changes;
    }

    private function recordPivotWrite(bool $changed): void
    {
        NormCache::invalidator()->recordPivotWrite(
            $this->parent->getConnection()->getName(),
            $this->table,
            [$this->parent::class, $this->related::class],
            $changed,
        );
    }
}
