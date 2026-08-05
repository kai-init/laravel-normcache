<?php

namespace NormCache\Planning;

use Illuminate\Database\Query\Builder;
use NormCache\Values\TableIdentity;

final class DependencyCollection
{
    /** @var array<string, TableIdentity> */
    private array $tables = [];

    /** @var array<int, true> */
    private array $visited = [];

    private bool $opaque = false;

    public function add(TableIdentity $table): void
    {
        $this->tables[$table->hash] = $table;
    }

    public function enter(Builder $query): bool
    {
        $id = spl_object_id($query);

        if (isset($this->visited[$id])) {
            return false;
        }

        $this->visited[$id] = true;

        return true;
    }

    public function markOpaque(): void
    {
        $this->opaque = true;
    }

    public function isOpaque(): bool
    {
        return $this->opaque;
    }

    /** @return list<TableIdentity> */
    public function all(): array
    {
        ksort($this->tables, SORT_STRING);

        return array_values($this->tables);
    }
}
