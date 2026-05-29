<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
use NormCache\Facades\NormCache;

/**
 * @mixin Model
 */
trait Cacheable
{
    use CachesRelationships;

    private bool $withoutCacheNext = false;

    public function flush(): void
    {
        NormCache::flushInstance($this);
    }

    // -------------------------------------------------------------------------
    // Public overrides
    // -------------------------------------------------------------------------

    public function newEloquentBuilder($query)
    {
        if (!config('normcache.enabled', true)) {
            return parent::newEloquentBuilder($query);
        }

        $builder = new CacheableBuilder($query);

        if ($this->withoutCacheNext) {
            $this->withoutCacheNext = false;

            $builder->withoutCache();
        }

        return $builder;
    }

    public function refresh(): static
    {
        return $this->runWithoutCache(fn() => parent::refresh());
    }

    public function fresh($with = []): ?static
    {
        return $this->runWithoutCache(fn() => parent::fresh($with));
    }

    public function save(array $options = []): bool
    {
        $existsBefore = $this->exists;
        $result = parent::save($options);

        if ($result) {
            $this->invalidateAfterSave($existsBefore);
        }

        return $result;
    }

    public function saveQuietly(array $options = []): bool
    {
        $existsBefore = $this->exists;
        $result = parent::saveQuietly($options);

        if ($result) {
            $this->invalidateAfterSave($existsBefore);
        }

        return $result;
    }

    public function delete(): ?bool
    {
        $result = parent::delete();
        $this->flushIfDeleted($result);

        return $result;
    }

    public function deleteQuietly(): bool
    {
        $result = parent::deleteQuietly();
        $this->flushIfDeleted($result);

        return $result;
    }

    public function increment($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(
            parent::increment($column, $amount, $extra)
        );
    }

    public function decrement($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(
            parent::decrement($column, $amount, $extra)
        );
    }

    public function incrementQuietly($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(
            parent::incrementQuietly($column, $amount, $extra)
        );
    }

    public function decrementQuietly($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(
            parent::decrementQuietly($column, $amount, $extra)
        );
    }

    public function incrementEach(array $columns, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(
            parent::incrementEach($columns, $extra)
        );
    }

    public function decrementEach(array $columns, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(
            parent::decrementEach($columns, $extra)
        );
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function invalidateAfterSave(bool $existsBefore): void
    {
        if (!$existsBefore && $this->wasRecentlyCreated) {
            NormCache::invalidateVersion($this);
        } elseif ($this->isRestoreSave($existsBefore)) {
            NormCache::invalidateVersion($this);
        } elseif ($existsBefore && $this->wasChanged()) {
            NormCache::flushInstance($this);
        }
    }

    private function flushIfDeleted(?bool $result): void
    {
        if ($result) {
            NormCache::flushInstance($this);
        }
    }

    private function flushAfterCounterMutation(int|bool $result): int
    {
        if ($result) {
            $this->exists
                ? NormCache::flushInstance($this)
                : NormCache::flushModel($this);
        }

        return (int) $result;
    }

    private function isRestoreSave(bool $existsBefore): bool
    {
        if (!$existsBefore || !method_exists($this, 'getDeletedAtColumn')) {
            return false;
        }

        $deletedAtColumn = $this->getDeletedAtColumn();

        return $this->wasChanged($deletedAtColumn) && $this->{$deletedAtColumn} === null;
    }

    private function runWithoutCache(callable $callback)
    {
        $previous = $this->withoutCacheNext;
        $this->withoutCacheNext = true;

        try {
            return $callback();
        } finally {
            $this->withoutCacheNext = $previous;
        }
    }
}
