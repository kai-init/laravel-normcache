<?php

namespace NormCache;

use Illuminate\Contracts\Database\Query\Expression;
use NormCache\Cache\CacheRuntime;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\MutationType;
use NormCache\Exceptions\CascadeException;
use NormCache\Planning\CascadeDependencyResolver;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\TableIdentityResolver;
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
        private readonly TableIdentityResolver $tables,
        private readonly CascadeDependencyResolver $cascades,
        private readonly PrimaryKeyResolver $primaryKeys,
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

        $table = $this->tables->resolve($connection, $from);

        if ($table === null) {
            $this->failures->opaqueWriteGlobalInvalidation($connectionName);

            if ($connection->transactionLevel() > 0) {
                $this->pendingGlobalInvalidations[$connectionName] = 'transaction_opaque_write';

                return;
            }

            $this->applyGlobal('opaque_write');

            return;
        }

        $cascadeTables = [];

        if ($mutation === MutationType::DELETE) {
            try {
                $cascadeTables = $this->cascades->affectedByDelete($connection, $table);

                if ($cascadeTables === null) {
                    if ($connection->transactionLevel() > 0) {
                        $this->pendingGlobalInvalidations[$connectionName] = 'transaction_cascade_graph_cold';
                    } else {
                        $this->applyGlobal('cascade_graph_cold');
                    }

                    try {
                        $this->cascades->warm($connection);
                    } catch (CascadeException $failure) {
                        $this->failures->cascadeGlobalInvalidation($table, $failure);
                    }

                    return;
                }
            } catch (CascadeException $failure) {
                $this->failures->cascadeGlobalInvalidation($table, $failure);

                if ($connection->transactionLevel() > 0) {
                    $this->pendingGlobalInvalidations[$connectionName] = 'transaction_cascade_metadata_unavailable';

                    return;
                }

                $this->applyGlobal('cascade_metadata_unavailable');

                return;
            }
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
        $mode = $broad ? 'generation' : ($tokens === [] ? 'version' : 'precise');

        $this->observer->begin();

        try {
            $this->store->invalidateTableState(
                versionKey: $this->keys->version($table),
                generationKey: $this->keys->generation($table),
                mode: $mode,
                tokens: $tokens,
                rowPrefix: $this->keys->tablePrefix($table) . ':r:g',
            );
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
            $table = $invalidation['table'];
            $tokens = $invalidation['tokens'];
            $mode = $invalidation['broad']
                ? 'generation'
                : ($tokens === [] ? 'version' : 'precise');
            $states[] = [
                'versionKey' => $this->keys->version($table),
                'generationKey' => $this->keys->generation($table),
                'mode' => $mode,
                'tokens' => $tokens,
                'rowPrefix' => $this->keys->tablePrefix($table) . ':r:g',
            ];
        }

        $first = $invalidations[0];
        $firstMode = $states[0]['mode'];

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
            $this->runtime->disable();
            $this->failures->invalidationFailed(
                $exception,
                $first['table'],
                $firstMode,
                $first['tokens'],
            );

            return false;
        }
    }
}
