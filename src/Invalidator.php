<?php

namespace NormCache;

use NormCache\Cache\CacheRuntime;
use NormCache\Database\QueryBuilder;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Support\Reporter;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;

final class Invalidator
{
    /** @var array<string, array<string, array{table: TableIdentity, broad: bool, tokens: array<string, true>}>> */
    private array $pendingInvalidations = [];

    public function __construct(
        private readonly CacheConfig $config,
        private readonly CacheRuntime $runtime,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly TableIdentityResolver $tables,
        private readonly PrimaryKeyResolver $primaryKeys,
        private readonly MutationKeyExtractor $mutationKeys,
        private readonly Reporter $reporter,
    ) {}

    /** @param array<string, mixed>|null $assigned */
    public function afterWrite(
        QueryBuilder $query,
        bool $mayAffectExistingRows,
        bool $forceBroadInvalidation = false,
        ?array $assigned = null,
    ): void {
        if (!$this->runtime->invalidating()) {
            return;
        }

        $connection = $query->getConnection();
        $table = $this->tables->resolve($connection, $query->from);

        if ($table === null) {
            return;
        }

        $broad = $forceBroadInvalidation;
        $tokens = [];

        if ($mayAffectExistingRows && !$forceBroadInvalidation) {
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
            $this->queueInvalidation($table, $broad, $tokens);

            return;
        }

        $this->apply($table, $broad, $tokens);
    }

    public function commit(string $connection): void
    {
        foreach ($this->pullInvalidations($connection) as $pending) {
            $tokens = array_keys($pending['tokens']);
            $broad = $pending['broad']
                || count($tokens) > $this->config->maxPreciseInvalidationKeys;
            $this->apply($pending['table'], $broad, $tokens);
        }
    }

    public function rollback(string $connection): void
    {
        unset($this->pendingInvalidations[$connection]);
    }

    public function invalidateTable(TableIdentity $table): bool
    {
        if (!$this->runtime->invalidating()) {
            return false;
        }

        return $this->apply($table, true, []);
    }

    /** @param list<string> $tokens */
    private function queueInvalidation(TableIdentity $table, bool $broad, array $tokens): void
    {
        $current = $this->pendingInvalidations[$table->connection][$table->hash] ?? null;
        $tokenSet = $current['tokens'] ?? [];

        foreach ($tokens as $token) {
            $tokenSet[$token] = true;
        }

        $this->pendingInvalidations[$table->connection][$table->hash] = [
            'table' => $table,
            'broad' => $broad || ($current['broad'] ?? false),
            'tokens' => $tokenSet,
        ];
    }

    /** @return list<array{table: TableIdentity, broad: bool, tokens: array<string, true>}> */
    private function pullInvalidations(string $connection): array
    {
        $pending = array_values($this->pendingInvalidations[$connection] ?? []);
        unset($this->pendingInvalidations[$connection]);

        return $pending;
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
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }
}
