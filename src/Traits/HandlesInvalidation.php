<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\CacheManager;
use NormCache\Support\CacheKeyBuilder;

/**
 * @phpstan-require-extends CacheManager
 */
trait HandlesInvalidation
{
    /** @var array<string, array<string, true>> */
    private array $flushQueue = [];

    /** @var array<string, array<string, true>> */
    private array $versionQueue = [];

    // -------------------------------------------------------------------------
    // Invalidation
    // -------------------------------------------------------------------------

    public function invalidateVersion(Model $model): void
    {
        if (!$this->enabled) {
            return;
        }

        $conn = $model->getConnection()->getName();

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueVersionFlush($conn, $this->keys->classKey($model::class));

            return;
        }

        $this->attempt(fn() => $this->doInvalidateVersion($model::class));
    }

    public function flushModel(Model|string $model): void
    {
        if (!$this->enabled) {
            return;
        }

        if (is_string($model)) {
            $this->attempt(fn() => $this->forceFlushModel($model));

            return;
        }

        $conn = $model->getConnection()->getName();

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueModelFlush($conn, $model::class);

            return;
        }

        $this->attempt(fn() => $this->forceFlushModel($model::class));
    }

    public function flushInstance(Model $model): void
    {
        if (!$this->enabled) {
            return;
        }

        $conn = $model->getConnection()->getName();
        $class = $model::class;
        $key = $this->keys->modelKey($class, $model->getKey());

        if (DB::connection($conn)->transactionLevel() > 0) {
            $this->queueModelFlush($conn, $class);

            return;
        }

        $this->attempt(function () use ($class, $key) {
            $classKey = $this->keys->classKey($class);
            $this->doInvalidateVersion($class);
            $this->store->deleteFromSet(
                $key,
                $this->keys->membersKey($classKey)
            );
        });
    }

    public function invalidateTableVersion(string $connectionName, string $table): void
    {
        if (!$this->enabled) {
            return;
        }

        $classKey = $this->keys->tableKey($connectionName, $table);

        if (DB::connection($connectionName)->transactionLevel() > 0) {
            $this->queueVersionFlush($connectionName, $classKey);

            return;
        }

        $this->attempt(fn() => $this->doInvalidateKey($classKey));
    }

    public function evictModelKey(string $modelClass, mixed $id): void
    {
        if (!$this->enabled) {
            return;
        }

        $this->attempt(function () use ($modelClass, $id) {
            $classKey = $this->keys->classKey($modelClass);
            $key = $this->keys->modelPrefix($classKey) . $id;
            $this->store->deleteFromSet(
                $key,
                $this->keys->membersKey($classKey)
            );
        });
    }

    public function forceFlushModel(string $modelClass): void
    {
        $classKey = $this->keys->classKey($modelClass);
        $this->store->increment($this->keys->verKey($classKey)); // bypass cooldown

        $this->store->sscanAndFlushSet($this->store->prefix($this->keys->membersKey($classKey)));
    }

    public function flushAll(): int
    {
        return $this->store->flushByPatterns([
            CacheKeyBuilder::K_QUERY . ':*',
            CacheKeyBuilder::K_MODEL . ':*',
            CacheKeyBuilder::K_MEMBERS . ':*',
            CacheKeyBuilder::K_VER . ':*',
            CacheKeyBuilder::K_COUNT . ':*',
            CacheKeyBuilder::K_SCALAR . ':*',
            CacheKeyBuilder::K_PIVOT . ':*',
            CacheKeyBuilder::K_THROUGH . ':*',
            CacheKeyBuilder::K_SCHEDULED . ':*',
            CacheKeyBuilder::K_BUILDING . ':*',
            CacheKeyBuilder::K_WAKE . ':*',
            CacheKeyBuilder::K_RESULT . ':*',
        ]);
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        if (preg_match('/[:{}\s*]/', $tag)) {
            throw new \InvalidArgumentException('Cache tag must not contain reserved characters (: { } * or whitespace).');
        }

        $classKey = $this->keys->classKey($modelClass);

        return $this->store->flushByPatterns([
            CacheKeyBuilder::K_RESULT . ':{' . $classKey . '}:' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':{' . $classKey . '}:' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':{' . $classKey . '}:' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':{' . $classKey . '}:' . $tag . ':*',
        ]);
    }

    public function flushTagAcrossModels(string $tag): int
    {
        if (preg_match('/[:{}\s*]/', $tag)) {
            throw new \InvalidArgumentException('Cache tag must not contain reserved characters (: { } * or whitespace).');
        }

        return $this->store->flushByPatterns([
            CacheKeyBuilder::K_RESULT . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':*:' . $tag . ':*',
        ]);
    }

    public function invalidateMultipleVersions(array $modelClasses, ?string $connectionName = null): void
    {
        if (!$this->enabled) {
            return;
        }

        if ($connectionName !== null && DB::connection($connectionName)->transactionLevel() > 0) {
            foreach ($modelClasses as $modelClass) {
                $this->queueVersionFlush($connectionName, $this->keys->classKey($modelClass));
            }

            return;
        }

        $this->attempt(function () use ($modelClasses) {
            foreach ($modelClasses as $modelClass) {
                $this->doInvalidateVersion($modelClass);
            }
        });
    }

    public function commitPending(string $connectionName): void
    {
        $flushes = array_keys($this->flushQueue[$connectionName] ?? []);
        $versions = array_keys($this->versionQueue[$connectionName] ?? []);

        unset($this->flushQueue[$connectionName]);
        unset($this->versionQueue[$connectionName]);

        if ((empty($flushes) && empty($versions)) || !$this->enabled) {
            return;
        }

        $this->attempt(function () use ($flushes, $versions) {
            foreach ($flushes as $modelClass) {
                $this->forceFlushModel($modelClass);
            }

            foreach ($versions as $classKey) {
                $this->doInvalidateKey($classKey);
            }
        });
    }

    public function discardPending(string $connectionName): void
    {
        unset($this->flushQueue[$connectionName]);
        unset($this->versionQueue[$connectionName]);
    }

    public function discardAllPending(): void
    {
        $this->flushQueue = [];
        $this->versionQueue = [];
    }

    // -------------------------------------------------------------------------
    // Private — invalidation internals
    // -------------------------------------------------------------------------

    private function doInvalidateVersion(string $modelClass): void
    {
        $this->doInvalidateKey($this->keys->classKey($modelClass));
    }

    private function doInvalidateKey(string $classKey): void
    {
        if ($this->cooldown <= 0) {
            $this->store->increment($this->keys->verKey($classKey));

            return;
        }

        $this->resolveCurrentVersion($classKey);
        $this->scheduleInvalidation($classKey);
    }

    private function resolveCurrentVersion(string $classKey): string|int|null
    {
        if ($this->cooldown <= 0) {
            return $this->store->getRaw($this->keys->verKey($classKey));
        }

        return $this->luaFetchVersionWithCooldown($classKey, (int) floor(microtime(true) * 1000));
    }

    private function scheduleInvalidation(string $classKey): void
    {
        $dueAtMs = (int) floor(microtime(true) * 1000) + ($this->cooldown * 1000);

        $this->store->setNx($this->keys->scheduledKey($classKey), (string) $dueAtMs);
    }

    private function queueModelFlush(string $connectionName, string $modelClass): void
    {
        $this->flushQueue[$connectionName][$modelClass] = true;
    }

    private function queueVersionFlush(string $connectionName, string $classKey): void
    {
        $this->versionQueue[$connectionName][$classKey] = true;
    }
}
