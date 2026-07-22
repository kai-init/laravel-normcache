<?php

namespace NormCache\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\Enums\WriteOperation;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheReporter;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\CacheSpace;
use NormCache\Values\ModelWriteState;

final class Invalidator
{
    /** @var array<string, array<string, true>> */
    private array $flushQueue = [];

    /** @var array<string, array<string, list<CacheSpace>>> */
    private array $versionQueue = [];

    /** @var array<string, array<class-string<Model>, true>> */
    private array $modelVersionQueue = [];

    /** @var array<string, array<string, true>> */
    private array $tableVersionQueue = [];

    public function __construct(
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly CacheConfig $config,
        private readonly CacheSpaceRegistry $spaceRegistry,
        private readonly VersionStore $versions,
    ) {}

    public function beginModelSave(Model $model, bool $observeBeforeWrite = true): ModelWriteState
    {
        $preInvalidated = $observeBeforeWrite
            && $model->exists
            && $model->isDirty()
            && $model->getConnection()->transactionLevel() === 0
            && !$this->isPendingRestoreSave($model);

        if ($preInvalidated) {
            $this->invalidateVersion($model);
        }

        return new ModelWriteState($model->exists, $preInvalidated);
    }

    public function completeModelSave(Model $model, ModelWriteState $state, bool $succeeded): void
    {
        if (!$succeeded || !$this->modelWriteChanged($model, $state->existed) || $state->preInvalidated) {
            return;
        }

        $this->invalidateVersion($model);
    }

    public function recordBuilderWrite(Model $model, WriteOperation $operation, bool $changed): void
    {
        if (!$changed) {
            return;
        }

        if ($operation->isInsert()) {
            $this->invalidateVersion($model);

            return;
        }

        if ($model->exists) {
            $this->invalidateVersion($model);

            return;
        }

        $this->flushModel($model);
    }

    public function recordPivotWrite(
        string $connection,
        string $table,
        array $modelClasses,
        bool $changed,
    ): void {
        if (!$changed) {
            return;
        }

        $this->invalidatePivotTableVersion($connection, $table, $modelClasses);
    }

    public function invalidateVersion(Model $model): void
    {
        if (!$this->config->enabled) {
            return;
        }

        $connection = $model->getConnection()->getName();
        $this->queueOrRun(
            $connection,
            fn() => $this->modelVersionQueue[$connection][$model::class] = true,
            fn() => $this->invalidateModelNow($model::class, $connection),
        );
    }

    public function flushModel(Model|string $model): void
    {
        if (!$this->config->enabled) {
            return;
        }

        $modelClass = is_string($model) ? $model : $model::class;
        $connection = is_string($model)
            ? ($this->keys->prototype($modelClass)->getConnectionName() ?? DB::getDefaultConnection())
            : $model->getConnection()->getName();

        $this->queueOrRun(
            $connection,
            fn() => $this->flushQueue[$connection][$modelClass] = true,
            fn() => $this->forceFlushModel($modelClass, $connection),
        );
    }

    public function invalidateTableVersion(string $connection, string $table): void
    {
        if (!$this->config->enabled) {
            return;
        }

        $classKey = $this->keys->tableKey($connection, $table);
        $this->queueOrRun(
            $connection,
            fn() => $this->tableVersionQueue[$connection][$classKey] = true,
            fn() => $this->invalidateTableNow($classKey),
        );
    }

    public function invalidatePivotTableVersion(string $connection, string $table, array $modelClasses): void
    {
        if (!$this->config->enabled) {
            return;
        }

        $classKey = $this->keys->tableKey($connection, $table);
        $spaces = [];
        foreach ($modelClasses as $modelClass) {
            foreach ($this->modelSpaces($modelClass, freshTableSpaces: true) as $space) {
                $spaces[$space->name] = $space;
            }
        }
        $spaces = array_values($spaces);

        $this->queueOrRun(
            $connection,
            fn() => $this->queueVersionFlush($connection, $classKey, $spaces),
            function () use ($classKey, $spaces): void {
                foreach ($spaces as $space) {
                    $this->versions->bump($classKey, $this->config, $space);
                }
                $this->reportBumps('pivot_table', $classKey, $spaces);
            },
        );
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connection = null): void
    {
        if (!$this->config->enabled || $modelClasses === []) {
            return;
        }

        $groups = [];
        foreach ($modelClasses as $modelClass) {
            $resolved = $connection
                ?? ($this->keys->prototype($modelClass)->getConnectionName() ?? DB::getDefaultConnection());
            $groups[$resolved][] = $modelClass;
        }

        foreach ($groups as $resolved => $classes) {
            $this->queueOrRun(
                $resolved,
                function () use ($resolved, $classes): void {
                    foreach ($classes as $modelClass) {
                        $this->modelVersionQueue[$resolved][$modelClass] = true;
                    }
                },
                function () use ($resolved, $classes): void {
                    foreach ($classes as $modelClass) {
                        $this->invalidateModelNow($modelClass, $resolved);
                    }
                },
            );
        }
    }

    public function forceFlushModel(string $modelClass, ?string $connection = null): void
    {
        $classKey = $this->keys->classKey($modelClass, $connection);
        $spaces = $this->modelSpaces($modelClass, $connection, true);
        foreach ($spaces as $space) {
            $this->versions->forceBump($classKey, $this->versions->versionTtl($this->config), $space);
        }
        $this->reportBumps('model_flush', $modelClass, $spaces);
    }

    public function flushAll(CacheSpace|string|null $space = null): int
    {
        $patterns = [
            CacheKeyBuilder::K_QUERY . ':*', CacheKeyBuilder::K_MODEL . ':*',
            CacheKeyBuilder::K_VER . ':*', CacheKeyBuilder::K_COUNT . ':*',
            CacheKeyBuilder::K_SCALAR . ':*', CacheKeyBuilder::K_PIVOT . ':*',
            CacheKeyBuilder::K_THROUGH . ':*', CacheKeyBuilder::K_SCHEDULED . ':*',
            CacheKeyBuilder::K_BUILDING . ':*', CacheKeyBuilder::K_WAKE . ':*',
            CacheKeyBuilder::K_RESULT . ':*',
        ];

        if (!$this->store->isCluster() && $space === null) {
            $count = $this->store->flushByPatterns($this->prefixedForAnySpace($patterns));
            CacheReporter::invalidation('flush_all', '*', $count);

            return $count;
        }

        $spaces = $space === null
            ? $this->spaceRegistry->knownSpaces()
            : [is_string($space) ? $this->spaceRegistry->space($space) : $space];

        $count = $this->store->flushByPatterns($this->prefixedForSpaces($patterns, $spaces));
        CacheReporter::invalidation('flush_all', '*', $count, array_column($spaces, 'name'));

        return $count;
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);
        $classKey = $this->keys->classKey($modelClass);

        $spaces = $this->modelSpaces($modelClass, freshTableSpaces: true);
        $count = $this->store->flushByPatterns($this->prefixedForSpaces([
            CacheKeyBuilder::K_RESULT . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_THROUGH . ':' . $classKey . ':' . $tag . ':*',
        ], $spaces));
        CacheReporter::invalidation('tag', $modelClass . ':' . $tag, $count, array_column($spaces, 'name'));

        return $count;
    }

    public function flushTagAcrossModels(string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);
        $patterns = [
            CacheKeyBuilder::K_RESULT . ':*:' . $tag . ':*', CacheKeyBuilder::K_QUERY . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':*:' . $tag . ':*', CacheKeyBuilder::K_SCALAR . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_THROUGH . ':*:' . $tag . ':*',
        ];

        $spaces = $this->store->isCluster() ? $this->spaceRegistry->knownSpaces() : [];
        $count = $this->store->flushByPatterns(
            !$this->store->isCluster()
                ? $this->prefixedForAnySpace($patterns)
                : $this->prefixedForSpaces($patterns, $spaces),
        );
        CacheReporter::invalidation('tag', '*:' . $tag, $count, array_column($spaces, 'name'));

        return $count;
    }

    public function commitPending(string $connection): void
    {
        $flushes = array_keys($this->flushQueue[$connection] ?? []);
        $models = array_keys($this->modelVersionQueue[$connection] ?? []);
        $versions = $this->versionQueue[$connection] ?? [];
        $tables = array_keys($this->tableVersionQueue[$connection] ?? []);
        $this->discardPending($connection);

        if (($flushes === [] && $models === [] && $versions === [] && $tables === []) || !$this->config->enabled) {
            return;
        }

        CacheFallback::attempt($this->config, function () use ($connection, $flushes, $models, $versions, $tables): void {
            /** @var array<string, array<string, true>> $invalidated */
            $invalidated = [];
            /** @var array<string, array<string, CacheSpace>> $pending */
            $pending = [];
            /** @var array<string, array<string, string>> $pendingTypes */
            $pendingTypes = [];
            $queue = function (
                string $classKey,
                array $spaces,
                string $dependencyType,
                string $target,
            ) use (&$pending, &$pendingTypes, &$invalidated): void {
                foreach ($spaces as $space) {
                    if (!isset($invalidated[$classKey][$space->name])) {
                        $pending[$classKey][$space->name] = $space;
                    }
                }
                $pendingTypes[$classKey][$dependencyType] = $target;
            };

            foreach ($flushes as $modelClass) {
                $classKey = $this->keys->classKey($modelClass, $connection);
                $spaces = $this->modelSpaces($modelClass, $connection, true);
                $this->forceFlushModel($modelClass, $connection);
                foreach ($spaces as $space) {
                    $invalidated[$classKey][$space->name] = true;
                }
            }
            foreach ($models as $modelClass) {
                $queue(
                    $this->keys->classKey($modelClass, $connection),
                    $this->modelSpaces($modelClass, $connection, true),
                    'model',
                    $modelClass,
                );
            }
            foreach ($versions as $classKey => $spaces) {
                $queue($classKey, $spaces, 'pivot_table', $classKey);
            }
            foreach ($tables as $classKey) {
                $queue($classKey, $this->spaceRegistry->freshSpacesForTable($classKey), 'table', $classKey);
            }
            foreach ($pending as $classKey => $spaces) {
                foreach ($spaces as $space) {
                    $this->versions->bump($classKey, $this->config, $space);
                }
                foreach ($pendingTypes[$classKey] ?? [] as $dependencyType => $target) {
                    $this->reportBumps($dependencyType, $target, array_values($spaces));
                }
            }
        });
    }

    public function discardPending(string $connection): void
    {
        unset($this->flushQueue[$connection], $this->modelVersionQueue[$connection], $this->versionQueue[$connection], $this->tableVersionQueue[$connection]);
    }

    public function discardAllPending(): void
    {
        $this->flushQueue = $this->modelVersionQueue = $this->versionQueue = $this->tableVersionQueue = [];
    }

    /** @return list<CacheSpace> */
    public function modelSpaces(string $modelClass, ?string $connection = null, bool $freshTableSpaces = false): array
    {
        $connection ??= $this->keys->declaredConnection($modelClass);
        $tableKey = $this->keys->classKey($modelClass, $connection);
        $tableSpaces = $freshTableSpaces
            ? $this->spaceRegistry->freshSpacesForTable($tableKey)
            : $this->spaceRegistry->spacesForTable($tableKey);
        $spaces = [];
        foreach ([...$this->spaceRegistry->spacesForModel($modelClass), ...$tableSpaces] as $space) {
            $spaces[$space->name] = $space;
        }

        return array_values($spaces);
    }

    private function invalidateModelNow(string $modelClass, ?string $connection = null): void
    {
        $classKey = $this->keys->classKey($modelClass, $connection);
        $spaces = $this->modelSpaces($modelClass, $connection);
        foreach ($spaces as $space) {
            $this->versions->bump($classKey, $this->config, $space);
        }
        $this->reportBumps('model', $modelClass, $spaces);
    }

    private function invalidateTableNow(string $classKey): void
    {
        $spaces = $this->spaceRegistry->freshSpacesForTable($classKey);
        foreach ($spaces as $space) {
            $this->versions->bump($classKey, $this->config, $space);
        }
        $this->reportBumps('table', $classKey, $spaces);
    }

    private function queueOrRun(?string $connection, callable $queue, callable $immediate): void
    {
        if ($connection !== null && DB::connection($connection)->transactionLevel() > 0) {
            $queue();

            return;
        }
        CacheFallback::attempt($this->config, $immediate);
    }

    private function queueVersionFlush(string $connection, string $classKey, array $spaces): void
    {
        $queued = [];
        foreach ($this->versionQueue[$connection][$classKey] ?? [] as $space) {
            $queued[$space->name] = $space;
        }
        foreach ($spaces as $space) {
            $queued[$space->name] = $space;
        }
        $this->versionQueue[$connection][$classKey] = array_values($queued);
    }

    private function reportBumps(string $dependencyType, string $target, array $spaces): void
    {
        CacheReporter::invalidation(
            $dependencyType,
            $target,
            count($spaces),
            array_column($spaces, 'name'),
        );
    }

    private function modelWriteChanged(Model $model, bool $existed): bool
    {
        if (!$existed) {
            return $model->wasRecentlyCreated;
        }

        if ($this->isCompletedRestoreSave($model)) {
            return true;
        }

        return $model->wasChanged();
    }

    private function isPendingRestoreSave(Model $model): bool
    {
        if (!method_exists($model, 'getDeletedAtColumn')) {
            return false;
        }

        $column = $model->getDeletedAtColumn();

        return $model->isDirty($column) && $model->getAttribute($column) === null;
    }

    private function isCompletedRestoreSave(Model $model): bool
    {
        if (!method_exists($model, 'getDeletedAtColumn')) {
            return false;
        }

        $column = $model->getDeletedAtColumn();

        return $model->wasChanged($column) && $model->getAttribute($column) === null;
    }

    private function prefixedForSpaces(array $patterns, array $spaces): array
    {
        $prefixed = [];
        foreach ($spaces as $space) {
            foreach ($patterns as $pattern) {
                $prefixed[] = $this->keys->prefixed($pattern, $space);
            }
        }

        return $prefixed;
    }

    private function prefixedForAnySpace(array $patterns): array
    {
        return array_map(
            fn(string $pattern) => preg_replace('/^\{[^}]+\}:/', '{*}:', $this->keys->prefixed($pattern)),
            $patterns,
        );
    }
}
