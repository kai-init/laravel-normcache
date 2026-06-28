<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;
use NormCache\CacheableBuilder;
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

    // Cache spaces this model belongs to (empty = default), via $normCacheSpaces.
    /** @return list<string> */
    public static function normCacheSpaces(): array
    {
        return property_exists(static::class, 'normCacheSpaces') ? (array) static::$normCacheSpaces : [];
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

        NormCache::flushInstance($this);
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
