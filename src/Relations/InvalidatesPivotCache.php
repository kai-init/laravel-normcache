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
            $this->invalidatePivotCache();
        }
    }

    public function detach($ids = null, $touch = true): int
    {
        $result = parent::detach($ids, $touch);

        if (!$this->syncing) {
            $this->invalidatePivotCache();
        }

        return $result;
    }

    public function updateExistingPivot($id, array $attributes, $touch = true): int
    {
        $result = parent::updateExistingPivot($id, $attributes, $touch);
        $this->invalidatePivotCache();

        return $result;
    }

    public function sync($ids, $detaching = true): array
    {
        $this->syncing = true;

        try {
            $result = parent::sync($ids, $detaching);
        } finally {
            $this->syncing = false;
        }

        $this->invalidatePivotCache();

        return $result;
    }

    private function invalidatePivotCache(): void
    {
        $conn = $this->parent->getConnectionName();
        NormCache::invalidateVersion($this->parent::class, $conn);
        NormCache::invalidateVersion($this->related::class, $conn);
    }
}
