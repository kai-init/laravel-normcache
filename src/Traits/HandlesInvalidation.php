<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\CacheManager;
use NormCache\Support\CacheFallback;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Values\CacheSpace;

/**
 * @phpstan-require-extends CacheManager
 */
trait HandlesInvalidation
{
    /** @var array<string, array<string, true>> */
    private array $flushQueue = [];

    /** @var array<string, array<string, list<CacheSpace>>> conn => classKey => spaces */
    private array $versionQueue = [];

    /** @var array<string, array<string, true>> conn => table classKey => true */
    private array $tableVersionQueue = [];

    /** @return list<CacheSpace> the spaces a model's version key lives in */
    private function modelSpaces(string $modelClass): array
    {
        return $this->spaceRegistry->spacesForModel($modelClass);
    }

    /** @return list<CacheSpace> the spaces a table's version key lives in */
    private function tableInvalidationSpaces(string $tableKey): array
    {
        return $this->spaceRegistry->freshSpacesForTable($tableKey);
    }

    /** @return list<CacheSpace> */
    private function knownSpaces(): array
    {
        return $this->spaceRegistry->knownSpaces();
    }

    // Invalidation

    public function invalidateVersion(Model $model): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $conn = $model->getConnection()->getName();

        $this->queueOrRun(
            $conn,
            fn() => $this->queueVersionFlush($conn, $this->keys->classKey($model::class, $conn), $this->modelSpaces($model::class)),
            fn() => $this->doInvalidateVersion($model::class, $conn),
        );
    }

    public function flushModel(Model|string $model): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $modelClass = is_string($model) ? $model : $model::class;
        $conn = is_string($model)
            ? ($this->keys->prototype($modelClass)->getConnectionName() ?? DB::getDefaultConnection())
            : $model->getConnection()->getName();

        $this->queueOrRun(
            $conn,
            fn() => $this->queueModelFlush($conn, $modelClass),
            fn() => $this->forceFlushModel($modelClass, $conn),
        );
    }

    /** Alias of invalidateVersion(); kept as the model-instance-facing entry point. */
    public function flushInstance(Model $model): void
    {
        $this->invalidateVersion($model);
    }

    public function invalidateTableVersion(string $connectionName, string $table): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $classKey = $this->keys->tableKey($connectionName, $table);

        $this->queueOrRun(
            $connectionName,
            fn() => $this->queueTableVersionFlush($connectionName, $classKey),
            fn() => $this->doInvalidateTable($classKey),
        );
    }

    /** Bump the pivot table version only in the spaces belonging to the involved models. */
    public function invalidatePivotTableVersion(string $connectionName, string $table, array $modelClasses): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $classKey = $this->keys->tableKey($connectionName, $table);
        $spaces = [];
        foreach ($modelClasses as $modelClass) {
            foreach ($this->modelSpaces($modelClass) as $space) {
                $spaces[$space->name] = $space;
            }
        }
        $spaces = array_values($spaces);

        $this->queueOrRun(
            $connectionName,
            fn() => $this->queueVersionFlush($connectionName, $classKey, $spaces),
            function () use ($classKey, $spaces) {
                foreach ($spaces as $space) {
                    $this->doInvalidateKey($classKey, $space);
                }
            },
        );
    }

    public function forceFlushModel(string $modelClass, ?string $connectionName = null): void
    {
        $classKey = $this->keys->classKey($modelClass, $connectionName);

        foreach ($this->modelSpaces($modelClass) as $space) {
            $this->store->incrementAndExpire($this->keys->verKey($classKey, $space), $this->versionTtl()); // bypass cooldown
        }
    }

    public function flushAll(CacheSpace|string|null $space = null): int
    {
        $patterns = [
            CacheKeyBuilder::K_QUERY . ':*',
            CacheKeyBuilder::K_MODEL . ':*',
            CacheKeyBuilder::K_VER . ':*',
            CacheKeyBuilder::K_COUNT . ':*',
            CacheKeyBuilder::K_SCALAR . ':*',
            CacheKeyBuilder::K_PIVOT . ':*',
            CacheKeyBuilder::K_THROUGH . ':*',
            CacheKeyBuilder::K_SCHEDULED . ':*',
            CacheKeyBuilder::K_BUILDING . ':*',
            CacheKeyBuilder::K_WAKE . ':*',
            CacheKeyBuilder::K_RESULT . ':*',
        ];

        if (!$this->store->isCluster()) {
            if ($space === null) {
                return $this->store->flushByPatterns($this->prefixedForAnySpace($patterns));
            }
        }

        $spaces = $space === null
            ? $this->knownSpaces()
            : [is_string($space) ? $this->spaceRegistry->space($space) : $space];

        return $this->store->flushByPatterns($this->prefixedForSpaces($patterns, $spaces));
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);

        $classKey = $this->keys->classKey($modelClass);

        return $this->store->flushByPatterns($this->prefixedForSpaces([
            CacheKeyBuilder::K_RESULT . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_THROUGH . ':' . $classKey . ':' . $tag . ':*',
        ], $this->modelSpaces($modelClass)));
    }

    public function flushTagAcrossModels(string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);

        $patterns = [
            CacheKeyBuilder::K_RESULT . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_THROUGH . ':*:' . $tag . ':*',
        ];

        if (!$this->store->isCluster()) {
            return $this->store->flushByPatterns($this->prefixedForAnySpace($patterns));
        }

        return $this->store->flushByPatterns($this->prefixedForSpaces($patterns, $this->knownSpaces()));
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connectionName = null): void
    {
        if (!$this->isEnabled() || $modelClasses === []) {
            return;
        }

        $groups = [];

        foreach ($modelClasses as $modelClass) {
            $conn = $connectionName
                ?? ($this->keys->prototype($modelClass)->getConnectionName() ?? DB::getDefaultConnection());
            $groups[$conn][] = $modelClass;
        }

        foreach ($groups as $conn => $classes) {
            $this->queueOrRun(
                $conn,
                function () use ($conn, $classes) {
                    foreach ($classes as $modelClass) {
                        $this->queueVersionFlush($conn, $this->keys->classKey($modelClass, $conn), $this->modelSpaces($modelClass));
                    }
                },
                function () use ($classes, $conn) {
                    foreach ($classes as $modelClass) {
                        $this->doInvalidateVersion($modelClass, $conn);
                    }
                },
            );
        }
    }

    public function commitPending(string $connectionName): void
    {
        $flushes = array_keys($this->flushQueue[$connectionName] ?? []);
        $versions = $this->versionQueue[$connectionName] ?? [];
        $tables = array_keys($this->tableVersionQueue[$connectionName] ?? []);

        unset(
            $this->flushQueue[$connectionName],
            $this->versionQueue[$connectionName],
            $this->tableVersionQueue[$connectionName],
        );

        if ((empty($flushes) && empty($versions) && empty($tables)) || !$this->isEnabled()) {
            return;
        }

        CacheFallback::attempt(
            $this->config,
            function () use ($connectionName, $flushes, $versions, $tables) {
                foreach ($flushes as $modelClass) {
                    $this->forceFlushModel($modelClass, $connectionName);
                }

                $flushClassKeys = array_map(fn($class) => $this->keys->classKey($class, $connectionName), $flushes);
                $tableClassKeys = array_fill_keys($tables, true);

                foreach ($versions as $classKey => $spaces) {
                    if (!in_array($classKey, $flushClassKeys, true) && !isset($tableClassKeys[$classKey])) {
                        foreach ($spaces as $space) {
                            $this->doInvalidateKey($classKey, $space);
                        }
                    }
                }

                foreach ($tables as $classKey) {
                    $this->doInvalidateTable($classKey);
                }
            },
        );
    }

    public function discardPending(string $connectionName): void
    {
        unset(
            $this->flushQueue[$connectionName],
            $this->versionQueue[$connectionName],
            $this->tableVersionQueue[$connectionName],
        );
    }

    public function discardAllPending(): void
    {
        $this->flushQueue = [];
        $this->versionQueue = [];
        $this->tableVersionQueue = [];
    }

    // Private — invalidation internals
    private function queueOrRun(?string $connectionName, callable $queue, callable $immediate): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            $queue();

            return;
        }

        CacheFallback::attempt($this->config, $immediate);
    }

    private function doInvalidateVersion(string $modelClass, ?string $connectionName = null): void
    {
        $classKey = $this->keys->classKey($modelClass, $connectionName);

        foreach ($this->modelSpaces($modelClass) as $space) {
            $this->doInvalidateKey($classKey, $space);
        }
    }

    private function doInvalidateTable(string $classKey): void
    {
        foreach ($this->tableInvalidationSpaces($classKey) as $space) {
            $this->doInvalidateKey($classKey, $space);
        }
    }

    private function doInvalidateKey(string $classKey, ?CacheSpace $space = null): void
    {
        if ($this->config->cooldown <= 0) {
            $this->store->incrementAndExpire($this->keys->verKey($classKey, $space), $this->versionTtl());

            return;
        }

        $this->resolveCurrentVersion($classKey, $space);
        $this->scheduleInvalidation($classKey, $space);
    }

    private function versionTtl(): int
    {
        return max($this->config->ttl, $this->config->queryTtl) * 2;
    }

    private function resolveCurrentVersion(string $classKey, ?CacheSpace $space = null): string|int|null
    {
        if ($this->config->cooldown <= 0) {
            return $this->store->getRaw($this->keys->verKey($classKey, $space));
        }

        return $this->store->fetchVersionWithCooldown(
            $this->keys->verKey($classKey, $space),
            $this->keys->scheduledKey($classKey, $space)
        );
    }

    private function scheduleInvalidation(string $classKey, ?CacheSpace $space = null): void
    {
        $dueAtMs = (int) floor(microtime(true) * 1000) + ($this->config->cooldown * 1000);

        $this->store->setNxEx($this->keys->scheduledKey($classKey, $space), (string) $dueAtMs, $this->config->cooldown + $this->versionTtl());
    }

    private function queueModelFlush(string $connectionName, string $modelClass): void
    {
        $this->flushQueue[$connectionName][$modelClass] = true;
    }

    /**
     * @param  list<string>  $patterns
     * @param  list<CacheSpace>  $spaces
     * @return list<string>
     */
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

    /**
     * @param  list<string>  $patterns
     * @return list<string>
     */
    private function prefixedForAnySpace(array $patterns): array
    {
        return array_map(
            fn(string $pattern) => preg_replace('/^\{[^}]+\}:/', '{*}:', $this->keys->prefixed($pattern)),
            $patterns,
        );
    }

    /** @param  list<CacheSpace>  $spaces */
    private function queueVersionFlush(string $connectionName, string $classKey, array $spaces): void
    {
        $queued = [];

        foreach ($this->versionQueue[$connectionName][$classKey] ?? [] as $space) {
            $queued[$space->name] = $space;
        }

        foreach ($spaces as $space) {
            $queued[$space->name] = $space;
        }

        $this->versionQueue[$connectionName][$classKey] = array_values($queued);
    }

    private function queueTableVersionFlush(string $connectionName, string $classKey): void
    {
        $this->tableVersionQueue[$connectionName][$classKey] = true;
    }
}
