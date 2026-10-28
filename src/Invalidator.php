<?php

namespace NormCache;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\PendingInvalidations;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\MutationType;
use NormCache\Planning\DeleteDependencyResolver;
use NormCache\Planning\MutationKeyExtractor;
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
    public function __construct(
        private readonly CacheConfig $config,
        private readonly CacheRuntime $runtime,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly TableIdentityResolver $tables,
        private readonly DeleteDependencyResolver $deleteDependencies,
        private readonly MutationKeyExtractor $mutationKeys,
        private readonly QueryObserver $observer,
        private readonly FailureReporter $failures,
        private readonly PendingInvalidations $pending,
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

        try {
            $connection = $query->getConnection();
            $connectionName = (string) $connection->getName();
            $inPhysicalTx = $this->inPhysicalTransaction($connection);

            if (!$inPhysicalTx && $this->hasPending($connectionName)) {
                $this->reconcileOrphaned($connection);
            }

            $from = $query->from;

            if ($from instanceof Expression) {
                $from = $from->getValue($query->getGrammar());
            }

            $table = $this->tables->resolve($connection, $from);

            if ($table === null) {
                $this->applyOrQueueGlobal($connection, 'opaque_write');
                $this->failures->opaqueWriteGlobalInvalidation($connectionName);

                return;
            }

            $truncating = $mutation === MutationType::TRUNCATE;
            $affectedByDelete = [];

            if ($truncating || $mutation === MutationType::DELETE) {
                try {
                    $epoch = $this->runtime->epoch();
                    $resolved = $truncating
                        ? $this->deleteDependencies->affectedByTruncate($connection, $table, $epoch)
                        : $this->deleteDependencies->affectedByDelete($connection, $table, $epoch);
                } catch (\Throwable) {
                    $resolved = null;
                }

                if ($resolved === null) {
                    $reason = $mutation->value . '_dependencies_unavailable';
                    $this->applyOrQueueGlobal($connection, $reason);
                    $this->failures->deleteDependencyGlobalInvalidation($table, $mutation);

                    return;
                }

                $affectedByDelete = $resolved;
            }

            $broad = $forceBroadInvalidation;
            $tokens = [];

            if ($mayAffectExistingRows && !$forceBroadInvalidation) {
                $primaryKey = $query->primaryKey();
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

            $invalidations = [[
                'table' => $table,
                'broad' => $broad,
                'tokens' => $tokens,
            ]];

            foreach ($affectedByDelete as $affected) {
                if ($affected->hash === $table->hash) {
                    $invalidations[0]['broad'] = true;

                    continue;
                }

                $invalidations[] = [
                    'table' => $affected,
                    'broad' => true,
                    'tokens' => [],
                ];
            }

            if ($inPhysicalTx) {
                if ($truncating) {
                    $this->applyMany($invalidations);
                }

                foreach ($invalidations as $invalidation) {
                    $this->queueInvalidation(
                        $invalidation['table'],
                        $invalidation['broad'],
                        $invalidation['tokens'],
                        $connection,
                    );
                }

                return;
            }

            $this->applyMany($invalidations);
        } catch (\Throwable $exception) {
            $this->disableAfterInvalidationFailure();
            $this->failures->cacheUnavailable($exception);
        }
    }

    public function commit(string $connection): void
    {
        if (isset($this->pending->global[$connection])) {
            $reason = $this->pending->global[$connection];
            unset($this->pending->global[$connection]);
            $this->pullInvalidations($connection);
            $this->applyGlobal($reason);

            return;
        }

        $invalidations = [];

        foreach ($this->pullInvalidations($connection) as $pending) {
            $invalidations[] = [
                'table' => $pending['table'],
                'broad' => $pending['broad'],
                'tokens' => array_keys($pending['tokens']),
            ];
        }

        $this->applyMany($invalidations);
    }

    public function committing(string $connection): void
    {
        if ($this->hasPending($connection)) {
            $this->pending->commitAttempts[$connection] = true;
        }
    }

    public function rollback(string $connection): void
    {
        if (isset($this->pending->commitAttempts[$connection]) && $this->hasPending($connection)) {
            $this->reconcileOrphaned($this->pending->connections[$connection]);

            return;
        }

        unset(
            $this->pending->tables[$connection],
            $this->pending->global[$connection],
            $this->pending->connections[$connection],
            $this->pending->commitAttempts[$connection],
        );
    }

    public function hasPending(string $connection): bool
    {
        return isset($this->pending->tables[$connection])
            || isset($this->pending->global[$connection]);
    }

    public function reconcileOrphaned(Connection $connection): bool
    {
        $name = (string) $connection->getName();
        if (!$this->hasPending($name)) {
            return true;
        }

        $origin = $this->pending->connections[$name] ?? $connection;
        if ($this->inPhysicalTransaction($origin)) {
            return false;
        }

        $this->commit($name);

        return true;
    }

    public function reconcileAllOrphaned(): void
    {
        foreach ($this->pending->connections as $connection) {
            $this->reconcileOrphaned($connection);
        }
    }

    public function inPhysicalTransaction(Connection $connection): bool
    {
        try {
            return $connection->getPdo()->inTransaction();
        } catch (\Throwable $exception) {
            $this->disableAfterInvalidationFailure();
            $this->failures->cacheUnavailable($exception);

            return true;
        }
    }

    public function invalidateTables(array $tables): bool
    {
        if ($tables === [] || !$this->runtime->invalidating()) {
            return $tables === [];
        }

        $immediate = [];

        foreach ($tables as $table) {
            $connection = DB::connection($table->connection);
            if ($this->inPhysicalTransaction($connection)) {
                $this->queueInvalidation($table, true, [], $connection);

                continue;
            }

            $immediate[] = [
                'table' => $table,
                'broad' => true,
                'tokens' => [],
            ];
        }

        return $this->applyMany($immediate);
    }

    /** @param list<string> $tokens */
    private function queueInvalidation(TableIdentity $table, bool $broad, array $tokens, Connection $connection): void
    {
        if (isset($this->pending->global[$table->connection])) {
            return;
        }

        $current = $this->pending->tables[$table->connection][$table->hash] ?? null;
        $broad = $broad || ($current['broad'] ?? false);
        $tokenSet = $broad ? [] : ($current['tokens'] ?? []) + array_fill_keys($tokens, true);

        if (!$broad && count($tokenSet) > $this->config->maxPreciseInvalidationKeys) {
            $broad = true;
            $tokenSet = [];
        }

        $this->pending->connections[$table->connection] ??= $connection;
        $this->pending->tables[$table->connection][$table->hash] = [
            'table' => $table,
            'broad' => $broad,
            'tokens' => $tokenSet,
        ];
    }

    /** @return list<array{table: TableIdentity, broad: bool, tokens: array<string, true>}> */
    private function pullInvalidations(string $connection): array
    {
        $pending = array_values($this->pending->tables[$connection] ?? []);
        unset(
            $this->pending->tables[$connection],
            $this->pending->connections[$connection],
            $this->pending->commitAttempts[$connection],
        );

        return $pending;
    }

    private function applyOrQueueGlobal(Connection $connection, string $reason): void
    {
        if ($this->inPhysicalTransaction($connection)) {
            $connectionName = (string) $connection->getName();
            $this->pending->global[$connectionName] = 'transaction_' . $reason;
            $this->pending->connections[$connectionName] ??= $connection;
            unset($this->pending->tables[$connectionName]);

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
            $this->disableAfterInvalidationFailure();
            $this->failures->globalInvalidationFailed($exception, $reason);

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

        $states = $affected = [];

        foreach ($invalidations as $invalidation) {
            $table = $invalidation['table'];
            $tokens = $invalidation['tokens'];
            $mode = $invalidation['broad'] ? 'generation' : ($tokens === [] ? 'version' : 'precise');
            $states[] = [
                'versionKey' => $this->keys->version($table),
                'generationKey' => $this->keys->generation($table),
                'mode' => $mode,
                'tokens' => $tokens,
                'rowPrefix' => $this->keys->tablePrefix($table) . ':r:g',
            ];
            $affected[] = ['table' => $table, 'mode' => $mode, 'token_count' => count($tokens)];
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
            $this->disableAfterInvalidationFailure();
            $this->failures->invalidationFailed($exception, $affected);

            return false;
        }
    }

    private function disableAfterInvalidationFailure(): void
    {
        $this->runtime->disable();

        try {
            $this->store->setRawForever($this->keys->disabled(), '1');
        } catch (\Throwable $exception) {
            $this->failures->invalidationDisableFailed($exception);
        }
    }
}
