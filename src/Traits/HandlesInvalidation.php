<?php

namespace NormCache\Traits;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\CacheManager;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisScripts;

/**
 * @phpstan-require-extends CacheManager
 */
trait HandlesInvalidation
{
    /** @var array<string, array<string, true>> */
    private array $flushQueue = [];

    /** @var array<string, array<string, true>> */
    private array $versionQueue = [];

    // Invalidation

    public function invalidateVersion(Model $model): void
    {
        if (!$this->isEnabled()) {
            return;
        }

        $conn = $model->getConnection()->getName();

        $this->queueOrRun(
            $conn,
            fn() => $this->queueVersionFlush($conn, $this->keys->classKey($model::class)),
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
            fn() => $this->queueVersionFlush($conn, $this->keys->classKey($class)),
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
            fn() => $this->queueVersionFlush($connectionName, $classKey),
            fn() => $this->doInvalidateKey($classKey),
        );
    }

    public function forceFlushModel(string $modelClass): void
    {
        $classKey = $this->keys->classKey($modelClass);
        $this->store->incrementAndExpire($this->keys->verKey($classKey), $this->versionTtl()); // bypass cooldown
    }

    public function flushAll(): int
    {
        return $this->store->flushByPatterns([
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
        ]);
    }

    public function flushTag(string $modelClass, string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);

        $classKey = $this->keys->classKey($modelClass);

        return $this->store->flushByPatterns([
            CacheKeyBuilder::K_RESULT . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':' . $classKey . ':' . $tag . ':*',
            CacheKeyBuilder::K_THROUGH . ':' . $classKey . ':' . $tag . ':*',
        ]);
    }

    public function flushTagAcrossModels(string $tag): int
    {
        CacheKeyBuilder::assertValidTag($tag);

        return $this->store->flushByPatterns([
            CacheKeyBuilder::K_RESULT . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_QUERY . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_COUNT . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_SCALAR . ':*:' . $tag . ':*',
            CacheKeyBuilder::K_THROUGH . ':*:' . $tag . ':*',
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
                    $this->queueVersionFlush($connectionName, $this->keys->classKey($modelClass));
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
        $versions = array_keys($this->versionQueue[$connectionName] ?? []);

        unset($this->flushQueue[$connectionName], $this->versionQueue[$connectionName]);

        if ((empty($flushes) && empty($versions)) || !$this->isEnabled()) {
            return;
        }

        $this->attempt(function () use ($flushes, $versions) {
            foreach ($flushes as $modelClass) {
                $this->forceFlushModel($modelClass);
            }

            $flushClassKeys = array_map(fn($class) => $this->keys->classKey($class), $flushes);

            foreach ($versions as $classKey) {
                if (!in_array($classKey, $flushClassKeys, true)) {
                    $this->doInvalidateKey($classKey);
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
        $this->doInvalidateKey($this->keys->classKey($modelClass));
    }

    private function doInvalidateKey(string $classKey): void
    {
        if ($this->config->cooldown <= 0) {
            $this->store->incrementAndExpire($this->keys->verKey($classKey), $this->versionTtl());

            return;
        }

        $this->resolveCurrentVersion($classKey);
        $this->scheduleInvalidation($classKey);
    }

    private function versionTtl(): int
    {
        return max($this->config->ttl, $this->config->queryTtl) * 2;
    }

    private function resolveCurrentVersion(string $classKey): string|int|null
    {
        if ($this->config->cooldown <= 0) {
            return $this->store->getRaw($this->keys->verKey($classKey));
        }

        return $this->store->script(
            RedisScripts::get('fetch_version_with_cooldown'),
            [$this->keys->verKey($classKey), $this->keys->scheduledKey($classKey)],
            [(string) (int) floor(microtime(true) * 1000)]
        );
    }

    private function scheduleInvalidation(string $classKey): void
    {
        $dueAtMs = (int) floor(microtime(true) * 1000) + ($this->config->cooldown * 1000);

        $this->store->setNxEx($this->keys->scheduledKey($classKey), (string) $dueAtMs, $this->config->cooldown + $this->versionTtl());
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
