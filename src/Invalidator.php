<?php

namespace NormCache;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\Database\QueryBuilder;
use NormCache\Enums\MutationType;
use NormCache\Exceptions\TableInvalidationException;
use NormCache\Payload\ChangeRecordCodec;
use NormCache\Planning\DeleteDependencyResolver;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\ColumnName;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryObserver;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\TableIdentity;

final class Invalidator
{
    private const MAX_PRECISE_INVALIDATION_KEYS = 1000;

    /**
     * @var array<string, array<string, array{
     *     table: TableIdentity,
     *     broad: bool,
     *     tokens: array<string, true>,
     *     columns: list<string>,
     *     revalidatable: bool
     * }>>
     */
    private array $pendingInvalidations = [];

    /** @var array<string, string> */
    private array $pendingGlobalInvalidations = [];

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
        private readonly ChangeRecordCodec $changes,
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
            $resolved = $truncating
                ? $this->deleteDependencies->affectedByTruncate($connection, $table)
                : $this->deleteDependencies->affectedByDelete($connection, $table);

            if ($resolved === null) {
                $reason = strtolower($mutation->name) . '_dependencies_unavailable';
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
                || count($extracted) > self::MAX_PRECISE_INVALIDATION_KEYS
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
            'columns' => $assigned === null
                ? []
                : $this->changedColumns($assigned, $query->volatileColumns()),
            'revalidatable' => $this->config->revalidation
                && $mutation === MutationType::UPDATE
                && $assigned !== null
                && !$broad
                && $tokens !== [],
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
                'columns' => [],
                'revalidatable' => false,
            ];
        }

        if ($connection->transactionLevel() > 0) {
            if ($truncating) {
                $this->applyMany($invalidations);
            }

            foreach ($invalidations as $invalidation) {
                $this->queueInvalidation(
                    $invalidation['table'],
                    $invalidation['broad'],
                    $invalidation['tokens'],
                    $invalidation['columns'],
                    $invalidation['revalidatable'],
                );
            }

            return;
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
                    || count($tokens) > self::MAX_PRECISE_INVALIDATION_KEYS,
                'tokens' => $tokens,
                'columns' => $pending['columns'],
                'revalidatable' => $pending['revalidatable'],
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
        // A deliberate kill switch is not a failure to report to the caller.
        if ($tables === [] || !$this->runtime->invalidating()) {
            return true;
        }

        $immediate = [];

        foreach ($tables as $table) {
            if (DB::connection($table->connection)->transactionLevel() > 0) {
                $this->queueInvalidation($table, true, []);

                continue;
            }

            $immediate[] = [
                'table' => $table,
                'broad' => true,
                'tokens' => [],
                'columns' => [],
                'revalidatable' => false,
            ];
        }

        return $this->applyMany($immediate);
    }

    /**
     * @param  list<string>  $tokens
     * @param  list<string>  $columns
     */
    private function queueInvalidation(
        TableIdentity $table,
        bool $broad,
        array $tokens,
        array $columns = [],
        bool $revalidatable = false,
    ): void {
        $current = $this->pendingInvalidations[$table->connection][$table->hash] ?? null;
        $tokenSet = $current['tokens'] ?? [];

        foreach ($tokens as $token) {
            $tokenSet[$token] = true;
        }

        $this->pendingInvalidations[$table->connection][$table->hash] = [
            'table' => $table,
            'broad' => $broad || ($current['broad'] ?? false),
            'tokens' => $tokenSet,
            'columns' => array_values(array_unique([
                ...($current['columns'] ?? []),
                ...$columns,
            ])),
            'revalidatable' => ($current['revalidatable'] ?? true) && $revalidatable,
        ];
    }

    /**
     * @return list<array{
     *     table: TableIdentity,
     *     broad: bool,
     *     tokens: array<string, true>,
     *     columns: list<string>,
     *     revalidatable: bool
     * }>
     */
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

    /**
     * @param  list<string>  $tokens
     * @param  list<string>  $columns
     */
    private function apply(
        TableIdentity $table,
        bool $broad,
        array $tokens,
        array $columns = [],
        bool $revalidatable = false,
    ): bool {
        $state = $this->storeInvalidation($table, $broad, $tokens, $columns, $revalidatable);
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
     * @param  list<array{
     *     table: TableIdentity,
     *     broad: bool,
     *     tokens: list<string>,
     *     columns: list<string>,
     *     revalidatable: bool
     * }>  $invalidations
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
                $invalidation['columns'],
                $invalidation['revalidatable'],
            );
        }

        $states = [];

        foreach ($invalidations as $invalidation) {
            $states[] = $this->storeInvalidation(
                $invalidation['table'],
                $invalidation['broad'],
                $invalidation['tokens'],
                $invalidation['columns'],
                $invalidation['revalidatable'],
            );
        }

        $this->observer->begin();

        try {
            $this->store->invalidateTableStates($states);

            $observed = [];

            foreach ($invalidations as $index => $invalidation) {
                $observed[] = [
                    'table' => $invalidation['table'],
                    'mode' => $states[$index]['mode'],
                    'tokens' => $invalidation['tokens'],
                ];
            }

            $this->observer->invalidatedMany($observed);

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

    /** @param list<string> $columns */
    private function changeRecord(array $columns, bool $revalidatable, string $mode): string
    {
        if (!$revalidatable || $mode !== 'precise') {
            return '';
        }

        try {
            return $this->changes->encode(
                mutation: MutationType::UPDATE->value,
                columns: $columns,
                precise: true,
            );
        } catch (\Throwable) {
            // A missing record already fails closed on read.
            return '';
        }
    }

    /**
     * @param  array<string, mixed>  $assigned
     * @param  list<string>  $volatile
     * @return list<string>
     */
    private function changedColumns(array $assigned, array $volatile): array
    {
        $columns = [];

        foreach ([...array_keys($assigned), ...$volatile] as $column) {
            $column = (string) $column;
            $arrow = strpos($column, '->');

            // A data->k assignment changes the data column.
            if ($arrow !== false) {
                $column = substr($column, 0, $arrow);
            }

            $columns[ColumnName::unqualified($column)] = true;
        }

        return array_map(strval(...), array_keys($columns));
    }

    private function storeInvalidation(
        TableIdentity $table,
        bool $broad,
        array $tokens,
        array $columns = [],
        bool $revalidatable = false,
    ): array {
        $mode = $broad ? 'generation' : ($tokens === [] ? 'version' : 'precise');

        return [
            'versionKey' => $this->keys->version($table),
            'generationKey' => $this->keys->generation($table),
            'mode' => $mode,
            'tokens' => $tokens,
            'rowPrefix' => $this->keys->tablePrefix($table) . ':r:g',
            'changePrefix' => $this->keys->changeRecordPrefix($table),
            'changePayload' => $this->changeRecord($columns, $revalidatable, $mode),
            'changeTtl' => $this->config->queryTtl,
        ];
    }
}
