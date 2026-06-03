<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;
use NormCache\Facades\NormCache;
use NormCache\Relations\CachesRelationships;

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
        return $this->saveWithCacheInvalidation(fn() => parent::save($options));
    }

    public function saveQuietly(array $options = []): bool
    {
        return $this->saveWithCacheInvalidation(fn() => Model::withoutEvents(fn() => parent::save($options)));
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
        return $this->flushAfterCounterMutation(parent::increment($column, $amount, $extra));
    }

    public function decrement($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(parent::decrement($column, $amount, $extra));
    }

    public function incrementQuietly($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(parent::incrementQuietly($column, $amount, $extra));
    }

    public function decrementQuietly($column, $amount = 1, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(parent::decrementQuietly($column, $amount, $extra));
    }

    public function incrementEach(array $columns, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(parent::incrementEach($columns, $extra));
    }

    public function decrementEach(array $columns, array $extra = []): int
    {
        return $this->flushAfterCounterMutation(parent::decrementEach($columns, $extra));
    }

    protected function performInsert(Builder $query): bool
    {
        if (method_exists($query, 'withoutInvalidation')) {
            return $query->withoutInvalidation(fn() => parent::performInsert($query));
        }

        return parent::performInsert($query);
    }

    protected function performUpdate(Builder $query): bool
    {
        if (method_exists($query, 'withoutInvalidation')) {
            return $query->withoutInvalidation(fn() => parent::performUpdate($query));
        }

        return parent::performUpdate($query);
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function saveWithCacheInvalidation(callable $save): bool
    {
        $existsBefore = $this->exists;
        $originalKey = $existsBefore ? $this->getOriginal($this->getKeyName()) : null;

        $this->evictBeforeSaveForObservers($existsBefore, $originalKey);

        $result = $save();

        if ($result) {
            $this->invalidateAfterSave($existsBefore, $originalKey);
        }

        return $result;
    }

    private function invalidateAfterSave(bool $existsBefore, mixed $originalKey = null): void
    {
        if (!$existsBefore && $this->wasRecentlyCreated) {
            NormCache::invalidateVersion($this);

            return;
        }

        if ($this->isRestoreSave($existsBefore)) {
            NormCache::invalidateVersion($this);

            return;
        }

        if (!$existsBefore || !$this->wasChanged()) {
            return;
        }

        if ($originalKey !== null && $originalKey !== $this->getKey()) {
            NormCache::evictModelKey(static::class, $originalKey);
        }

        NormCache::flushInstance($this);
    }

    private function flushIfDeleted(?bool $result): void
    {
        if ($result) {
            NormCache::flushInstance($this);
        }
    }

    // Evict before save so observers do not read a stale model payload.
    private function evictBeforeSaveForObservers(bool $existsBefore, mixed $originalKey = null): void
    {
        if (!$existsBefore) {
            return;
        }

        if (!$this->isDirty()) {
            return;
        }

        if ($this->isPendingRestoreSave()) {
            return;
        }

        if (!NormCache::isEnabled()) {
            return;
        }

        if ($this->getConnection()->transactionLevel() !== 0) {
            return;
        }

        NormCache::evictModelKey(static::class, $originalKey ?? $this->getKey());
    }

    private function flushAfterCounterMutation(int|bool $result): int|bool
    {
        if ($result) {
            $this->exists
                ? NormCache::flushInstance($this)
                : NormCache::flushModel($this);
        }

        return $result;
    }

    private function isRestoreSave(bool $existsBefore): bool
    {
        if (!$existsBefore || !method_exists($this, 'getDeletedAtColumn')) {
            return false;
        }

        $deletedAtColumn = $this->getDeletedAtColumn();

        return $this->wasChanged($deletedAtColumn) && $this->{$deletedAtColumn} === null;
    }

    private function isPendingRestoreSave(): bool
    {
        if (!method_exists($this, 'getDeletedAtColumn')) {
            return false;
        }

        $deletedAtColumn = $this->getDeletedAtColumn();

        return $this->isDirty($deletedAtColumn) && $this->{$deletedAtColumn} === null;
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
