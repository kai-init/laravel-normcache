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
        NormCache::invalidator()->invalidateVersion($this);
    }

    /** @return list<string> */
    public static function normCacheSpaces(): array
    {
        return property_exists(static::class, 'normCacheSpaces') ? (array) static::$normCacheSpaces : [];
    }

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
        return $this->saveWithCacheInvalidation(
            fn() => parent::save($options),
            observeBeforeWrite: true,
        );
    }

    public function saveQuietly(array $options = []): bool
    {
        return $this->saveWithCacheInvalidation(
            fn() => Model::withoutEvents(fn() => parent::save($options)),
            observeBeforeWrite: false,
        );
    }

    protected function performInsert(Builder $query): bool
    {
        // $query is a plain Eloquent Builder when normcache.enabled is false; see newEloquentBuilder().
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

    private function saveWithCacheInvalidation(callable $save, bool $observeBeforeWrite): bool
    {
        $invalidator = NormCache::invalidator();
        $state = $invalidator->beginModelSave($this, $observeBeforeWrite);
        $result = $save();
        $invalidator->completeModelSave($this, $state, $result);

        return $result;
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
