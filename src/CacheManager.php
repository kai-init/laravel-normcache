<?php

namespace NormCache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\Exceptions\CascadeException;
use NormCache\Planning\CascadeDependencyResolver;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\TableIdentity;

final readonly class CacheManager
{
    public function __construct(
        private CacheConfig $config,
        private CacheRuntime $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private Invalidator $invalidator,
        private CascadeDependencyResolver $cascades,
        private SchemaRepository $schema,
        private TableIdentityResolver $tables,
        private PrimaryKeyResolver $primaryKeys,
        private QueryIdentity $identity,
        private FailureReporter $failures,
    ) {}

    /**
     * @param  Model|class-string<Model>|string|list<Model|class-string<Model>|string>  $targets
     */
    public function invalidate(Model|string|array $targets, ?string $connection = null): bool
    {
        $identities = [];
        $success = true;

        foreach (Arr::wrap($targets) as $target) {
            $identity = $this->invalidationIdentity($target, $connection);

            if ($identity === null) {
                $success = false;

                continue;
            }

            $identities[$identity->encoded] = $identity;
        }

        foreach ($identities as $identity) {
            $success = $this->invalidator->invalidateTable($identity) && $success;
        }

        return $success;
    }

    public function invalidateTable(string $connection, string $table): bool
    {
        return $this->invalidate($table, $connection);
    }

    /** @param list<string> $tables */
    public function invalidateTables(string $connection, array $tables): bool
    {
        return $this->invalidate($tables, $connection);
    }

    private function invalidationIdentity(mixed $target, ?string $connection): ?TableIdentity
    {
        if ($target instanceof Model) {
            return $this->modelInvalidationIdentity($target, $connection);
        }

        if (!is_string($target)) {
            throw new \InvalidArgumentException(
                'invalidate() expects Eloquent models, model class names, or table names.',
            );
        }

        if (is_a($target, Model::class, true)) {
            return $this->modelInvalidationIdentity(new $target, $connection);
        }

        return $this->tables->resolve(DB::connection($connection), $target);
    }

    private function modelInvalidationIdentity(Model $model, ?string $connection): ?TableIdentity
    {
        if ($connection !== null) {
            $model = clone $model;
            $model->setConnection($connection);
        }

        return $this->tables->resolve($model->getConnection(), $model->getTable());
    }

    public function flushTag(string $tag): bool
    {
        $hash = $this->identity->tagHash($tag);

        return $this->increment($this->keys->tagVersion($hash));
    }

    public function flushAll(): bool
    {
        $this->runtime->forgetEpoch();

        return $this->increment($this->keys->epoch(), force: true);
    }

    public function withoutCache(callable $callback): mixed
    {
        return $this->runtime->withoutCache($callback);
    }

    public function disableCache(): bool
    {
        if (!$this->config->enabled) {
            return false;
        }

        try {
            $this->store->setRawForever($this->keys->disabled(), '1');
            $this->runtime->forgetEpoch();

            return true;
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }

    public function enableCache(): ?int
    {
        if (!$this->config->enabled) {
            return null;
        }

        $this->runtime->forgetEpoch();

        try {
            return $this->store->enableCache(
                $this->keys->epoch(),
                $this->keys->disabled(),
            );
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return null;
        }
    }

    public function cacheDisabled(): bool
    {
        if (!$this->config->enabled) {
            return false;
        }

        try {
            return $this->store->getRaw($this->keys->disabled()) !== null;
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }

    public function clearSchema(): bool
    {
        $this->tables->clear();
        $this->primaryKeys->clear();

        return $this->schema->clear();
    }

    public function refreshSchema(?string $connection = null): bool
    {
        $cleared = $this->clearSchema();
        $flushed = $this->flushAll();

        if (!$cleared || !$flushed) {
            return false;
        }

        $connections = $connection === null
            ? DB::getConnections()
            : [$connection => DB::connection($connection)];

        if ($connections === []) {
            $default = DB::connection();
            $connections[(string) $default->getName()] = $default;
        }

        try {
            foreach ($connections as $database) {
                $this->cascades->warm($database);
            }
        } catch (CascadeException $failure) {
            $this->failures->cascadeGlobalInvalidation(null, $failure);

            return false;
        }

        return true;
    }

    private function increment(string $key, bool $force = false): bool
    {
        if (!$force && !$this->config->enabled) {
            return false;
        }

        try {
            $this->store->increment($key);

            return true;
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }
}
