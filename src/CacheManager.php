<?php

namespace NormCache;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\RuntimeState;
use Throwable;

final readonly class CacheManager
{
    public function __construct(
        private CacheConfig $config,
        private RuntimeState $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private Invalidator $invalidator,
        private TableIdentityResolver $tables,
        private PrimaryKeyResolver $primaryKeys,
        private QueryIdentity $identity,
    ) {}

    public function invalidateTable(string $connection, string $table): bool
    {
        $identity = $this->tables->resolve(DB::connection($connection), $table);

        return $identity !== null && $this->invalidator->invalidateTable($identity);
    }

    /** @param list<string> $tables */
    public function invalidateTables(string $connection, array $tables): bool
    {
        $success = true;

        foreach (array_values(array_unique($tables)) as $table) {
            $success = $this->invalidateTable($connection, $table) && $success;
        }

        return $success;
    }

    public function flushTag(string $tag): bool
    {
        $hash = $this->identity->tagHash($tag);

        return $this->increment($this->keys->tagVersion($hash));
    }

    public function flushAll(): bool
    {
        $this->runtime->forgetEpoch();

        return $this->increment($this->keys->epoch());
    }

    public function clearSchemaMetadata(?string $connection = null): void
    {
        $this->tables->clear($connection);
        $this->primaryKeys->clear($connection);
    }

    private function increment(string $key): bool
    {
        if (!$this->config->enabled || !$this->runtime->available()) {
            return false;
        }

        try {
            $this->store->increment($key);

            return true;
        } catch (Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }
}
