<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\CacheManager;
use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;
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

    private ?CacheSpaceRegistry $spaceRegistry = null;

    /** @return list<CacheSpace> the spaces a model's version key lives in */
    private function modelSpaces(string $modelClass): array
    {
        return ($this->spaceRegistry ??= app(CacheSpaceRegistry::class))->spacesForModel($modelClass);
    }

    /** @return list<CacheSpace> the spaces a table's version key lives in */
    private function tableSpaces(string $table): array
    {
        return ($this->spaceRegistry ??= app(CacheSpaceRegistry::class))->spacesForTable($table);
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
            fn() => $this->queueVersionFlush($conn, $this->keys->classKey($model::class), $this->modelSpaces($model::class)),
            fn() => $this->doInvalidateVersion($model::class),
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
            fn() => $this->forceFlushModel($modelClass),
        );
    }

    public function flushInstance(Model $model): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $conn = $model->getConnection()->getName();
        $class = $model::class;

        $this->queueOrRun(
            $conn,
            fn() => $this->queueVersionFlush($conn, $this->keys->classKey($class), $this->modelSpaces($class)),
            fn() => $this->doInvalidateVersion($class),
        );
    }

    public function invalidateTableVersion(string $connectionName, string $table): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $classKey = $this->keys->tableKey($connectionName, $table);

        $this->queueOrRun(
            $connectionName,
            fn() => $this->queueVersionFlush($connectionName, $classKey, $this->tableSpaces($table)),
            fn() => $this->doInvalidateTable($table, $classKey),
        );
    }

    public function forceFlushModel(string $modelClass): void
    {
        $classKey = $this->keys->classKey($modelClass);

        foreach ($this->modelSpaces($modelClass) as $space) {
            $this->store->incrementAndExpire($this->keys->verKey($classKey, $space), $this->versionTtl()); // bypass cooldown
        }
    }

    public function flushAll(): int
    {
        return $this->store->flushByPatterns([
            $this->keys->prefixed(CacheKeyBuilder::K_QUERY . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_MODEL . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_VER . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_COUNT . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_SCALAR . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_PIVOT . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_THROUGH . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_SCHEDULED . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_BUILDING . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_WAKE . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_RESULT . ':*'),
        ]);
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);

        $classKey = $this->keys->classKey($modelClass);

        return $this->store->flushByPatterns([
            $this->keys->prefixed(CacheKeyBuilder::K_RESULT . ':' . $classKey . ':' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_QUERY . ':' . $classKey . ':' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_COUNT . ':' . $classKey . ':' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_SCALAR . ':' . $classKey . ':' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_THROUGH . ':' . $classKey . ':' . $tag . ':*'),
        ]);
    }

    public function flushTagAcrossModels(string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);

        return $this->store->flushByPatterns([
            $this->keys->prefixed(CacheKeyBuilder::K_RESULT . ':*:' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_QUERY . ':*:' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_COUNT . ':*:' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_SCALAR . ':*:' . $tag . ':*'),
            $this->keys->prefixed(CacheKeyBuilder::K_THROUGH . ':*:' . $tag . ':*'),
        ]);
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connectionName = null): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $this->queueOrRun(
            $connectionName,
            function () use ($connectionName, $modelClasses) {
                foreach ($modelClasses as $modelClass) {
                    $this->queueVersionFlush($connectionName, $this->keys->classKey($modelClass), $this->modelSpaces($modelClass));
                }
            },
            function () use ($modelClasses) {
                foreach ($modelClasses as $modelClass) {
                    $this->doInvalidateVersion($modelClass);
                }
            },
        );
    }

    public function commitPending(string $connectionName): void
    {
        $flushes = array_keys($this->flushQueue[$connectionName] ?? []);
        $versions = $this->versionQueue[$connectionName] ?? [];

        unset($this->flushQueue[$connectionName], $this->versionQueue[$connectionName]);

        if ((empty($flushes) && empty($versions)) || !$this->isEnabled()) {
            return;
        }

        $this->attempt(function () use ($flushes, $versions) {
            foreach ($flushes as $modelClass) {
                $this->forceFlushModel($modelClass);
            }

            $flushClassKeys = array_map(fn($class) => $this->keys->classKey($class), $flushes);

            foreach ($versions as $classKey => $spaces) {
                if (!in_array($classKey, $flushClassKeys, true)) {
                    foreach ($spaces as $space) {
                        $this->doInvalidateKey($classKey, $space);
                    }
                }
            }
        });
    }

    public function discardPending(string $connectionName): void
    {
        unset($this->flushQueue[$connectionName], $this->versionQueue[$connectionName]);
    }

    public function discardAllPending(): void
    {
        $this->flushQueue = [];
        $this->versionQueue = [];
    }

    // Private — invalidation internals
    private function queueOrRun(?string $connectionName, callable $queue, callable $immediate): void
    {
        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            $queue();

            return;
        }

        $this->attempt($immediate);
    }

    private function doInvalidateVersion(string $modelClass): void
    {
        $classKey = $this->keys->classKey($modelClass);

        foreach ($this->modelSpaces($modelClass) as $space) {
            $this->doInvalidateKey($classKey, $space);
        }
    }

    private function doInvalidateTable(string $table, string $classKey): void
    {
        foreach ($this->tableSpaces($table) as $space) {
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

        return $this->store->script(
            RedisScripts::get('fetch_version_with_cooldown'),
            [$this->keys->verKey($classKey, $space), $this->keys->scheduledKey($classKey, $space)],
            [(string) (int) floor(microtime(true) * 1000)]
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

    /** @param  list<CacheSpace>  $spaces */
    private function queueVersionFlush(string $connectionName, string $classKey, array $spaces): void
    {
        $this->versionQueue[$connectionName][$classKey] = $spaces;
    }
}
