<?php

namespace NormCache;

use NormCache\Database\CachingQueryBuilder;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Support\Reporter;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\RuntimeState;
use NormCache\Values\TableIdentity;
use Throwable;

final readonly class Invalidator
{
    public function __construct(
        private CacheConfig $config,
        private RuntimeState $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
        private TableIdentityResolver $tables,
        private PrimaryKeyResolver $primaryKeys,
        private MutationKeyExtractor $mutationKeys,
        private Reporter $reporter,
    ) {}

    /** @param array<string, mixed>|null $assigned */
    public function afterWrite(
        CachingQueryBuilder $query,
        bool $mayAffectRows,
        bool $forceBroad = false,
        ?array $assigned = null,
    ): void {
        if (!$this->config->enabled) {
            return;
        }

        $connection = $query->getConnection();
        $table = $this->tables->resolve($connection, $query->from);

        if ($table === null) {
            return;
        }

        $broad = $forceBroad;
        $tokens = [];

        if ($mayAffectRows && !$forceBroad) {
            $primaryKey = $this->primaryKeys->resolve($query, $connection, $table);
            $extracted = $primaryKey !== null
                && $primaryKey->family === PrimaryKeyMetadata::INTEGER
                ? $this->mutationKeys->extractMutation($query, $primaryKey, $assigned)
                : null;

            if (
                $extracted === null
                || count($extracted) > $this->config->maxPreciseInvalidationKeys
            ) {
                $broad = true;
            } else {
                $tokens = $extracted;
            }
        }

        if ($connection->transactionLevel() > 0) {
            $this->runtime->queueInvalidation($table, $broad, $tokens);

            return;
        }

        $this->apply($table, $broad, $tokens);
    }

    public function commit(string $connection): void
    {
        foreach ($this->runtime->pullInvalidations($connection) as $pending) {
            $tokens = array_keys($pending['tokens']);
            $broad = $pending['broad']
                || count($tokens) > $this->config->maxPreciseInvalidationKeys;
            $this->apply($pending['table'], $broad, $tokens);
        }
    }

    public function rollback(string $connection): void
    {
        $this->runtime->discardInvalidations($connection);
    }

    public function invalidateTable(TableIdentity $table): bool
    {
        if (!$this->config->enabled) {
            return false;
        }

        return $this->apply($table, true, []);
    }

    /** @param list<string> $tokens */
    private function apply(TableIdentity $table, bool $broad, array $tokens): bool
    {
        try {
            $mode = $broad ? 'generation' : ($tokens === [] ? 'version' : 'precise');

            $this->store->invalidateTableState(
                versionKey: $this->keys->version($table),
                generationKey: $this->keys->generation($table),
                mode: $mode,
                tokens: $tokens,
                rowPrefix: $this->keys->tablePrefix($table) . ':r:g',
            );
            $this->reporter->invalidated($table, $mode, $tokens);

            return true;
        } catch (Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }
}
