<?php

namespace NormCache\Planning;

use Illuminate\Database\Query\Builder;
use NormCache\Values\TableIdentity;

final class DependencyCollection
{
    /** @var array<string, TableIdentity> */
    private array $tables = [];

    /** @var array<string, true> */
    private array $visited = [];

    private ?string $bypassReason = null;

    public function add(TableIdentity $table): void
    {
        $this->tables[$table->hash] = $table;
    }

    public function enter(Builder $query, bool $authoritative): bool
    {
        $id = spl_object_id($query) . ':' . (int) $authoritative;

        if (isset($this->visited[$id])) {
            return false;
        }

        $this->visited[$id] = true;

        return true;
    }

    public function markOpaque(): void
    {
        $this->bypassReason ??= 'unidentifiable_dependency';
    }

    public function markUnresolvedDeclaration(): void
    {
        $this->bypassReason = 'unresolvable_declared_dependency';
    }

    public function bypassReason(): ?string
    {
        return $this->bypassReason;
    }

    /** @return list<TableIdentity> */
    public function all(): array
    {
        ksort($this->tables, SORT_STRING);

        return array_values($this->tables);
    }
}
