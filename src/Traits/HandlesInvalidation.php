<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use NormCache\Cache\Invalidator;
use NormCache\Values\CacheSpace;

trait HandlesInvalidation
{
    abstract public function invalidator(): Invalidator;

    public function invalidateVersion(Model $model): void
    {
        $this->invalidator()->invalidateVersion($model);
    }

    public function flushModel(Model|string $model): void
    {
        $this->invalidator()->flushModel($model);
    }

    public function flushInstance(Model $model): void
    {
        $this->invalidator()->invalidateVersion($model);
    }

    public function invalidateTableVersion(string $connectionName, string $table): void
    {
        $this->invalidator()->invalidateTableVersion($connectionName, $table);
    }

    public function invalidatePivotTableVersion(string $connectionName, string $table, array $modelClasses): void
    {
        $this->invalidator()->invalidatePivotTableVersion($connectionName, $table, $modelClasses);
    }

    public function forceFlushModel(string $modelClass, ?string $connectionName = null): void
    {
        $this->invalidator()->forceFlushModel($modelClass, $connectionName);
    }

    public function flushAll(CacheSpace|string|null $space = null): int
    {
        return $this->invalidator()->flushAll($space);
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        return $this->invalidator()->flushTag($modelClass, $tag);
    }

    public function flushTagAcrossModels(string $tag): int
    {
        return $this->invalidator()->flushTagAcrossModels($tag);
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connectionName = null): void
    {
        $this->invalidator()->invalidateMultipleVersions($modelClasses, $connectionName);
    }

    public function commitPending(string $connectionName): void
    {
        $this->invalidator()->commitPending($connectionName);
    }

    public function discardPending(string $connectionName): void
    {
        $this->invalidator()->discardPending($connectionName);
    }

    public function discardAllPending(): void
    {
        $this->invalidator()->discardAllPending();
    }

    private function modelSpaces(string $modelClass, ?string $connectionName = null, bool $freshTableSpaces = false): array
    {
        return $this->invalidator()->modelSpaces($modelClass, $connectionName, $freshTableSpaces);
    }
}
