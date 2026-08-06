<?php

namespace NormCache;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use NormCache\Cache\CacheRuntime;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\MutationType;
use NormCache\Exceptions\CascadeException;
use NormCache\Exceptions\TableInvalidationException;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\SchemaCatalog;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;

final class Invalidator
{
    /** @var array<string, array<string, array{table: TableIdentity, broad: bool, tokens: array<string, true>}>> */
    private array $pendingInvalidations = [];

    /** @var array<string, string> */
    private array $pendingGlobalInvalidations = [];

    public function __construct(
        private readonly CacheConfig $config,
        private readonly CacheRuntime $runtime,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly SchemaCatalog $schema,
        private readonly MutationKeyExtractor $mutationKeys,
        private readonly QueryObserver $observer,
        private readonly FailureReporter $failures,
    ) {}

    /** @param array<string, mixed>|null $assigned */
    public function afterWrite(
        QueryBuilder $query,
        MutationType $mutation,
        bool $mayAffectExistingRows,
        bool $forceBroadInvalidation = false,
        ?array $assigned = null,
    ): void {
        if (!$this->runtime->invalidating()) {
            return;
        }

        $connection = $query->getConnection();
        $connectionName = (string) $connection->getName();

        if ($mutation === MutationType::TRUNCATE) {
            $this->applyGlobal('truncate');

            if ($connection->transactionLevel() > 0) {
                $this->pendingGlobalInvalidations[$connectionName] = 'transaction_truncate';
            }

            return;
        }

        $from = $query->from;

        if ($from instanceof Expression) {
            $from = $from->getValue($query->getGrammar());
        }

        $table = $this->schema->resolveTable($connection, $from);

        if ($table === null) {
            $this->failures->opaqueWriteGlobalInvalidation($connectionName);
            $this->applyOrQueueGlobal($connection, 'opaque_write');

            return;
        }

        $cascadeTables = [];

        if ($mutation === MutationType::DELETE) {
            try {
                $cascadeTables = $this->schema->affectedByDelete($connection, $table);
            } catch (CascadeException $failure) {
                $this->failures->cascadeGlobalInvalidation($table, $failure);
                $this->applyOrQueueGlobal($connection, 'cascade_metadata_unavailable');

                return;
            }

            if ($cascadeTables === null) {
                $this->applyOrQueueGlobal($connection, 'cascade_graph_cold');

                try {
                    $this->schema->warm($connection);
                } catch (CascadeException $failure) {
                    $this->failures->cascadeGlobalInvalidation($table, $failure);
                }

                return;
            }
        }

        $broad = $forceBroadInvalidation;
        $tokens = [];

        if ($mayAffectExistingRows && !$forceBroadInvalidation) {
            $primaryKey = $this->schema->resolvePrimaryKey($query, $connection, $table);
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

            foreach ($cascadeTables as $cascadeTable) {
                $this->queueInvalidation($cascadeTable, true, []);
            }

            return;
        }

        foreach ($cascadeTables as $cascadeTable) {
            if ($cascadeTable->hash === $table->hash) {
                $broad = true;

                break;
            }
        }

        if ($cascadeTables === []) {
            $this->apply($table, $broad, $tokens);

            return;
        }

        $invalidations = [[
            'table' => $table,
            'broad' => $broad,
            'tokens' => $tokens,
        ]];

        foreach ($cascadeTables as $cascadeTable) {
            if ($cascadeTable->hash !== $table->hash) {
                $invalidations[] = [
                    'table' => $cascadeTable,
                    'broad' => true,
                    'tokens' => [],
                ];
            }
        }

        $this->applyMany($invalidations);
    }

    public function commit(string $connection): void
    {
        if (isset($this->pendingGlobalInvalidations[$connection])) {
            $reason = $this->pendingGlobalInvalidations[$connection];
            unset($this->pendingGlobalInvalidations[$connection]);
            $this->pullInvalidations($connection);
            $this->applyGlobal($reason);

            return;
        }

        $invalidations = [];

        foreach ($this->pullInvalidations($connection) as $pending) {
            $tokens = array_keys($pending['tokens']);
            $invalidations[] = [
                'table' => $pending['table'],
                'broad' => $pending['broad']
                    || count($tokens) > $this->config->maxPreciseInvalidationKeys,
                'tokens' => $tokens,
            ];
        }

        $this->applyMany($invalidations);
    }

    public function rollback(string $connection): void
    {
        unset(
            $this->pendingInvalidations[$connection],
            $this->pendingGlobalInvalidations[$connection],
        );
    }

    public function invalidateTable(TableIdentity $table): bool
    {
        return $this->invalidateTables([$table]);
    }

    public function invalidateTables(array $tables): bool
    {
        if ($tables === [] || !$this->runtime->invalidating()) {
            return $tables === [];
        }

        return $this->applyMany(array_map(
            static fn(TableIdentity $table): array => [
                'table' => $table,
                'broad' => true,
                'tokens' => [],
            ],
            $tables,
        ));
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

    private function applyOrQueueGlobal(Connection $connection, string $reason): void
    {
        if ($connection->transactionLevel() > 0) {
            $connectionName = (string) $connection->getName();
            $this->pendingGlobalInvalidations[$connectionName] = 'transaction_' . $reason;

            return;
        }

        $this->applyGlobal($reason);
    }

    private function applyGlobal(string $reason): bool
    {
        $this->runtime->forgetEpoch();

        try {
            $this->store->increment($this->keys->epoch());

            return true;
        } catch (\Throwable $exception) {
            $this->runtime->disable();
            $this->failures->globalInvalidationFailed($exception, $reason);

            return false;
        }
    }

    /** @param list<string> $tokens */
    private function apply(TableIdentity $table, bool $broad, array $tokens): bool
    {
        $state = $this->storeInvalidation($table, $broad, $tokens);
        $mode = $state['mode'];

        $this->observer->begin();

        try {
            $this->store->invalidateTableState(...$state);
            $this->observer->invalidated($table, $mode, $tokens);

            return true;
        } catch (\Throwable $exception) {
            $this->runtime->disable();
            $this->failures->invalidationFailed($exception, $table, $mode, $tokens);

            return false;
        }
    }

    /**
     * @param  list<array{table: TableIdentity, broad: bool, tokens: list<string>}>  $invalidations
     */
    private function applyMany(array $invalidations): bool
    {
        if ($invalidations === []) {
            return true;
        }

        if (count($invalidations) === 1) {
            $invalidation = $invalidations[0];

            return $this->apply(
                $invalidation['table'],
                $invalidation['broad'],
                $invalidation['tokens'],
            );
        }

        $states = [];

        foreach ($invalidations as $invalidation) {
            $states[] = $this->storeInvalidation(
                $invalidation['table'],
                $invalidation['broad'],
                $invalidation['tokens'],
            );
        }

        $this->observer->begin();

        try {
            $this->store->invalidateTableStates($states);

            foreach ($invalidations as $index => $invalidation) {
                $this->observer->invalidated(
                    $invalidation['table'],
                    $states[$index]['mode'],
                    $invalidation['tokens'],
                );
            }

            return true;
        } catch (\Throwable $exception) {
            $failedIndex = $exception instanceof TableInvalidationException
                ? $exception->stateIndex
                : 0;
            $failed = $invalidations[$failedIndex] ?? $invalidations[0];
            $failure = $exception->getPrevious() ?? $exception;

            $this->runtime->disable();
            $this->failures->invalidationFailed(
                $failure,
                $failed['table'],
                $states[$failedIndex]['mode'] ?? $states[0]['mode'],
                $failed['tokens'],
            );

            return false;
        }
    }

    private function storeInvalidation(
        TableIdentity $table,
        bool $broad,
        array $tokens,
    ): array {
        return [
            'versionKey' => $this->keys->version($table),
            'generationKey' => $this->keys->generation($table),
            'mode' => $broad ? 'generation' : ($tokens === [] ? 'version' : 'precise'),
            'tokens' => $tokens,
            'rowPrefix' => $this->keys->tablePrefix($table) . ':r:g',
        ];
    }
}
