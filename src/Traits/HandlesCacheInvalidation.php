<?php

namespace NormCache\Traits;

use NormCache\Facades\NormCache;

trait HandlesCacheInvalidation
{
    // -------------------------------------------------------------------------
    // Inserts — version bump only, model cache unaffected
    // -------------------------------------------------------------------------

    public function insert(array $values): bool
    {
        return $this->coordinateInvalidation(false, fn() => parent::insert($values));
    }

    public function insertOrIgnore(array $values): int
    {
        return $this->coordinateInvalidation(false, fn() => parent::insertOrIgnore($values));
    }

    public function insertUsing(array $columns, $query): int
    {
        return $this->coordinateInvalidation(false, fn() => parent::insertUsing($columns, $query));
    }

    public function insertOrIgnoreUsing(array $columns, $query): int
    {
        return $this->coordinateInvalidation(false, fn() => parent::insertOrIgnoreUsing($columns, $query));
    }

    public function insertOrIgnoreReturning(array $values, array $returning = ['*'], $uniqueBy = null): mixed
    {
        return $this->coordinateInvalidation(false, fn() => parent::insertOrIgnoreReturning($values, $returning, $uniqueBy));
    }

    public function insertGetId(array $values, $sequence = null): int
    {
        $id = parent::insertGetId($values, $sequence);
        NormCache::invalidateVersion($this->model);

        return $id;
    }

    // -------------------------------------------------------------------------
    // Updates / deletes — flush model cache and bump version
    // -------------------------------------------------------------------------

    public function update(array $values): int
    {
        return $this->coordinateInvalidation(true, fn() => parent::update($values));
    }

    public function updateFrom(array $values): int
    {
        return $this->coordinateInvalidation(true, fn() => parent::updateFrom($values));
    }

    public function updateOrInsert(array $attributes, $values = []): bool
    {
        return (bool) $this->coordinateInvalidation(true, fn() => parent::updateOrInsert($attributes, $values));
    }

    public function upsert(array $values, $uniqueBy, $update = null): int
    {
        return $this->coordinateInvalidation(true, fn() => parent::upsert($values, $uniqueBy, $update));
    }

    public function delete(): mixed
    {
        return $this->coordinateInvalidation(true, fn() => parent::delete());
    }

    public function forceDelete(): mixed
    {
        return $this->coordinateInvalidation(true, fn() => parent::forceDelete());
    }

    public function touch($column = null): int|bool
    {
        return $this->coordinateInvalidation(true, fn() => parent::touch($column));
    }

    public function increment($column, $amount = 1, array $extra = []): int
    {
        return $this->coordinateInvalidation(true, fn() => parent::increment($column, $amount, $extra));
    }

    public function decrement($column, $amount = 1, array $extra = []): int
    {
        return $this->coordinateInvalidation(true, fn() => parent::decrement($column, $amount, $extra));
    }

    public function incrementEach(array $columns, array $extra = [])
    {
        return $this->coordinateInvalidation(true, fn() => parent::incrementEach($columns, $extra));
    }

    public function decrementEach(array $columns, array $extra = [])
    {
        return $this->coordinateInvalidation(true, fn() => parent::decrementEach($columns, $extra));
    }

    public function truncate(): void
    {
        parent::truncate();
        NormCache::flushModel($this->model);
    }

    // -------------------------------------------------------------------------
    // Private
    // -------------------------------------------------------------------------

    private function coordinateInvalidation(bool $isUpdate, callable $callback): mixed
    {
        $result = $callback();

        if ($result === 0 || $result === false) {
            return $result;
        }

        if (!$isUpdate) {
            NormCache::invalidateVersion($this->model);

            return $result;
        }

        if (!$this->model->exists) {
            NormCache::flushModel($this->model);
        }

        return $result;
    }
}
