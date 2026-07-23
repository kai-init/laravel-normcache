<?php

namespace NormCache\Traits;

use Illuminate\Support\Collection;
use NormCache\Enums\WriteOperation;
use NormCache\Facades\NormCache;

trait BuilderInvalidation
{
    private bool $suppressInvalidation = false;

    public function withoutInvalidation(callable $callback): mixed
    {
        $previous = $this->suppressInvalidation;
        $this->suppressInvalidation = true;

        try {
            return $callback();
        } finally {
            $this->suppressInvalidation = $previous;
        }
    }

    public function insert(array $values): bool
    {
        return $this->writeWithInvalidation(
            WriteOperation::Insert,
            fn() => parent::insert($values),
            fn(bool $inserted): bool => $values !== [] && $inserted,
        );
    }

    public function insertOrIgnore(array $values): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Insert,
            fn() => parent::insertOrIgnore($values),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function insertUsing(array $columns, $query): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Insert,
            fn() => parent::insertUsing($columns, $query),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function insertOrIgnoreUsing(array $columns, $query): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Insert,
            fn() => parent::insertOrIgnoreUsing($columns, $query),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function insertOrIgnoreReturning(array $values, array $returning = ['*'], $uniqueBy = null): mixed
    {
        return $this->writeWithInvalidation(
            WriteOperation::Insert,
            fn() => $this->toBase()->insertOrIgnoreReturning($values, $returning, $uniqueBy),
            fn(Collection $rows): bool => $rows->isNotEmpty(),
        );
    }

    public function insertGetId(array $values, $sequence = null): int
    {
        return (int) $this->writeWithInvalidation(
            WriteOperation::Insert,
            fn() => parent::insertGetId($values, $sequence),
            fn(int $id): bool => true,
        );
    }

    public function update(array $values): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Update,
            fn() => parent::update($values),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function updateFrom(array $values): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Update,
            fn() => $this->toBase()->updateFrom($values),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function updateOrInsert(array $attributes, $values = []): bool
    {
        return (bool) $this->writeWithInvalidation(
            WriteOperation::Update,
            fn() => $this->toBase()->updateOrInsert($attributes, $values),
            fn(bool $succeeded): bool => $succeeded,
        );
    }

    public function upsert(array $values, $uniqueBy, $update = null): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Update,
            fn() => parent::upsert($values, $uniqueBy, $update),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function delete(): mixed
    {
        return $this->writeWithInvalidation(
            WriteOperation::Delete,
            fn() => parent::delete(),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function forceDelete(): mixed
    {
        return $this->writeWithInvalidation(
            WriteOperation::Delete,
            fn() => parent::forceDelete(),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function touch($column = null): int|bool
    {
        return $this->writeWithInvalidation(
            WriteOperation::Increment,
            fn() => parent::touch($column),
            fn(int|bool $affected): bool => $affected !== false && $affected > 0,
        );
    }

    public function increment($column, $amount = 1, array $extra = []): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Increment,
            fn() => parent::increment($column, $amount, $extra),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function decrement($column, $amount = 1, array $extra = []): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Increment,
            fn() => parent::decrement($column, $amount, $extra),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function incrementEach(array $columns, array $extra = []): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Increment,
            fn(): int => $this->toBase()->incrementEach($columns, $this->addUpdatedAtColumn($extra)),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function decrementEach(array $columns, array $extra = []): int
    {
        return $this->writeWithInvalidation(
            WriteOperation::Increment,
            fn(): int => $this->toBase()->decrementEach($columns, $this->addUpdatedAtColumn($extra)),
            fn(int $affected): bool => $affected > 0,
        );
    }

    public function truncate(): void
    {
        parent::truncate();
        $this->recordWrite(WriteOperation::Truncate, true);
    }

    private function writeWithInvalidation(
        WriteOperation $operation,
        callable $callback,
        callable $changed,
    ): mixed {
        $result = $callback();
        $this->recordWrite($operation, $changed($result));

        return $result;
    }

    private function recordWrite(WriteOperation $operation, bool $changed): void
    {
        if ($this->suppressInvalidation) {
            return;
        }

        NormCache::invalidator()->recordBuilderWrite($this->model, $operation, $changed);
    }
}
