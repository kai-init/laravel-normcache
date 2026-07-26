<?php

namespace NormCache\Database;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder;
use InvalidArgumentException;
use NormCache\Cache\Engine;
use NormCache\Invalidator;
use NormCache\Support\QueryIdentity;
use NormCache\Support\Reporter;
use NormCache\Traits\Cacheable;
use NormCache\Values\PrimaryKeyMetadata;
use PDO;

final class CachingQueryBuilder extends Builder
{
    public const ORIGIN_CACHEABLE_MODEL = 'cacheable_model';

    public const ORIGIN_DB_TABLE = 'db_table';

    // Declared explicitly: Laravel 12's Query\Builder doesn't define fetchUsing()/$fetchUsing
    // (added in 13), but this class is used under both ^12.0 and ^13.0 per composer.json.
    public array $fetchUsing = [];

    private ?string $normCacheOrigin = null;

    /** @var class-string|null */
    private ?string $normCacheModelClass = null;

    private ?PrimaryKeyMetadata $normCachePrimaryKey = null;

    private ?string $normCacheDeletedAtColumn = null;

    private bool $normCacheSkipped = false;

    private ?int $normCacheTtl = null;

    private ?string $normCacheTag = null;

    /** @var list<string> */
    private array $normCacheDeclaredTables = [];

    /** @var list<class-string<Model>> */
    private array $normCacheDeclaredModels = [];

    private int $normCacheWriteDepth = 0;

    private bool $normCacheTerminalExecuted = false;

    private bool $normCacheWriteChanged = false;

    private bool $normCacheWriteForcedBroad = false;

    /** @var array<string, mixed>|null */
    private ?array $normCacheWriteAssignments = null;

    public function markDbTable(): static
    {
        $this->normCacheOrigin = self::ORIGIN_DB_TABLE;

        return $this;
    }

    /** @param class-string $modelClass */
    public function markCacheableModel(
        string $modelClass,
        string $keyName,
        string $keyType,
        ?string $deletedAtColumn = null,
    ): static {
        $this->normCacheOrigin = self::ORIGIN_CACHEABLE_MODEL;
        $this->normCacheModelClass = $modelClass;
        $this->normCachePrimaryKey = new PrimaryKeyMetadata(
            $keyName,
            $keyType === 'int' || $keyType === 'integer'
                ? PrimaryKeyMetadata::INTEGER
                : PrimaryKeyMetadata::STRING,
        );
        $this->normCacheDeletedAtColumn = $deletedAtColumn;

        return $this;
    }

    public function normCacheOrigin(): ?string
    {
        return $this->normCacheOrigin;
    }

    /** @return class-string|null */
    public function normCacheModelClass(): ?string
    {
        return $this->normCacheModelClass;
    }

    public function normCachePrimaryKey(): ?PrimaryKeyMetadata
    {
        return $this->normCachePrimaryKey;
    }

    public function normCacheDeletedAtColumn(): ?string
    {
        return $this->normCacheDeletedAtColumn;
    }

    public function withoutCache(): static
    {
        $this->normCacheSkipped = true;

        return $this;
    }

    public function isNormCacheSkipped(): bool
    {
        return $this->normCacheSkipped;
    }

    public function ttl(int $seconds): static
    {
        if ($seconds < 1) {
            throw new InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->normCacheTtl = $seconds;

        return $this;
    }

    public function normCacheTtl(): ?int
    {
        return $this->normCacheTtl;
    }

    public function tag(string $tag): static
    {
        (new QueryIdentity)->tagHash($tag);
        $this->normCacheTag = $tag;

        return $this;
    }

    public function normCacheTag(): ?string
    {
        return $this->normCacheTag;
    }

    /** @param array<mixed> $tables */
    public function dependsOnTables(array $tables): static
    {
        if ($tables === []) {
            throw new InvalidArgumentException(
                'dependsOnTables() requires at least one table name.'
            );
        }

        foreach ($tables as $table) {
            if (
                !is_string($table)
                || trim($table) === ''
                || preg_match('/[:{}\s*]/', $table) === 1
            ) {
                throw new InvalidArgumentException(
                    'dependsOnTables() expects table names without reserved characters (: { } * or whitespace).'
                );
            }

            $this->normCacheDeclaredTables[] = $table;
        }

        $this->normCacheDeclaredTables = array_values(array_unique($this->normCacheDeclaredTables));

        return $this;
    }

    /** @param array<mixed> $modelClasses */
    public function dependsOn(array $modelClasses): static
    {
        if ($modelClasses === []) {
            throw new InvalidArgumentException(
                'dependsOn() requires at least one model class.'
            );
        }

        foreach ($modelClasses as $class) {
            if (!is_string($class)) {
                throw new InvalidArgumentException(
                    'dependsOn() expects model class names, not model instances.'
                );
            }

            if (!is_a($class, Model::class, true)) {
                throw new InvalidArgumentException(
                    "dependsOn() class [{$class}] must be an Eloquent model."
                );
            }

            if (!in_array(Cacheable::class, class_uses_recursive($class), true)) {
                throw new InvalidArgumentException(
                    "dependsOn() class [{$class}] must use the NormCache\\Traits\\Cacheable trait."
                );
            }

            $this->normCacheDeclaredModels[] = $class;
        }

        $this->normCacheDeclaredModels = array_values(array_unique(
            $this->normCacheDeclaredModels,
        ));

        return $this;
    }

    /** @return list<string> */
    public function normCacheDeclaredTables(): array
    {
        return $this->normCacheDeclaredTables;
    }

    /** @return list<class-string<Model>> */
    public function normCacheDeclaredModels(): array
    {
        return $this->normCacheDeclaredModels;
    }

    protected function runSelect()
    {
        $sql = $this->toSql();
        $bindings = $this->getBindings();

        if ($this->mustBypassNormCache()) {
            if (($reason = $this->bypassReason()) !== null) {
                app(Reporter::class)->bypass($this, $reason, $sql, $bindings);
            }

            return $this->connection->select(
                $sql,
                $bindings,
                !$this->useWritePdo,
                $this->fetchUsing,
            );
        }

        return app(Engine::class)->select(
            $this,
            $sql,
            $bindings,
            'select',
            fn() => $this->connection->select(
                $sql,
                $bindings,
                !$this->useWritePdo,
                $this->fetchUsing,
            ),
            fn() => $this->connection->select($sql, $bindings, false, []),
        );
    }

    public function exists()
    {
        $this->applyBeforeQueryCallbacks();
        $sql = $this->grammar->compileExists($this);
        $bindings = $this->getBindings();

        $results = $this->mustBypassNormCache()
            ? $this->runBypassedExists($sql, $bindings)
            : app(Engine::class)->select(
                $this,
                $sql,
                $bindings,
                'exists',
                fn() => $this->connection->select($sql, $bindings, true),
                fn() => $this->connection->select($sql, $bindings, false, []),
            );

        if (!isset($results[0])) {
            return false;
        }

        $result = (array) $results[0];

        return (bool) $result['exists'];
    }

    public function cursor()
    {
        if ($this->normCacheOrigin !== null) {
            app(Reporter::class)->bypass(
                $this,
                'streaming_cursor',
                $this->toSql(),
                $this->getBindings(),
            );
        }

        return parent::cursor();
    }

    public function explain()
    {
        if ($this->normCacheOrigin !== null) {
            app(Reporter::class)->bypass(
                $this,
                'explain_query',
                $this->toSql(),
                $this->getBindings(),
            );
        }

        return parent::explain();
    }

    public function insert(array $values): bool
    {
        return $this->observeWrite(
            broad: false,
            forceWhenExecuted: false,
            operation: function () use ($values): bool {
                $result = parent::insert($values);
                $this->recordTerminal($values !== [], $values !== [] && $result);

                return $result;
            },
        );
    }

    public function insertOrIgnore(array $values): int
    {
        return $this->observeWrite(
            false,
            false,
            function () use ($values): int {
                $result = parent::insertOrIgnore($values);
                $this->recordTerminal($values !== [], $result > 0);

                return $result;
            },
        );
    }

    public function insertOrIgnoreReturning(array $values, array $returning = ['*'], $uniqueBy = null): mixed
    {
        return $this->observeWrite(
            false,
            false,
            function () use ($values, $returning, $uniqueBy): mixed {
                $result = parent::insertOrIgnoreReturning($values, $returning, $uniqueBy);
                $this->recordTerminal($values !== [], $result->isNotEmpty());

                return $result;
            },
        );
    }

    public function insertGetId(array $values, $sequence = null): int
    {
        return $this->observeWrite(
            false,
            true,
            function () use ($values, $sequence): int {
                $result = parent::insertGetId($values, $sequence);
                $this->recordTerminal(true, true);

                return $result;
            },
        );
    }

    public function insertUsing(array $columns, $query): int
    {
        return $this->observeWrite(
            false,
            false,
            function () use ($columns, $query): int {
                $result = parent::insertUsing($columns, $query);
                $this->recordTerminal(true, $result > 0);

                return $result;
            },
        );
    }

    public function insertOrIgnoreUsing(array $columns, $query): int
    {
        return $this->observeWrite(
            false,
            false,
            function () use ($columns, $query): int {
                $result = parent::insertOrIgnoreUsing($columns, $query);
                $this->recordTerminal(true, $result > 0);

                return $result;
            },
        );
    }

    public function update(array $values): int
    {
        return $this->observeWrite(
            true,
            false,
            function () use ($values): int {
                $result = parent::update($values);
                $this->recordTerminal(true, $result > 0);

                return $result;
            },
            assigned: $values,
        );
    }

    public function updateFrom(array $values): int
    {
        return $this->observeWrite(
            true,
            false,
            function () use ($values): int {
                $result = parent::updateFrom($values);
                $this->recordTerminal(true, $result > 0);

                return $result;
            },
            assigned: $values,
        );
    }

    public function updateOrInsert(array $attributes, $values = []): bool
    {
        return $this->observeWrite(
            true,
            true,
            fn(): bool => parent::updateOrInsert($attributes, $values),
            forceBroad: true,
        );
    }

    public function upsert(array $values, $uniqueBy, $update = null): int
    {
        return $this->observeWrite(
            true,
            true,
            function () use ($values, $uniqueBy, $update): int {
                $result = parent::upsert($values, $uniqueBy, $update);

                if ($values !== [] && !$this->normCacheTerminalExecuted) {
                    $this->recordTerminal(true, $result > 0);
                }

                return $result;
            },
            forceBroad: true,
        );
    }

    public function increment($column, $amount = 1, array $extra = []): int
    {
        return $this->observeWrite(
            true,
            true,
            fn(): int => parent::increment($column, $amount, $extra),
        );
    }

    public function incrementEach(array $columns, array $extra = [])
    {
        return $this->observeWrite(
            true,
            true,
            fn() => parent::incrementEach($columns, $extra),
        );
    }

    public function decrement($column, $amount = 1, array $extra = []): int
    {
        return $this->observeWrite(
            true,
            true,
            fn(): int => parent::decrement($column, $amount, $extra),
        );
    }

    public function decrementEach(array $columns, array $extra = [])
    {
        return $this->observeWrite(
            true,
            true,
            fn() => parent::decrementEach($columns, $extra),
        );
    }

    public function delete($id = null)
    {
        return $this->observeWrite(
            true,
            false,
            function () use ($id) {
                $result = parent::delete($id);
                $this->recordTerminal(true, $result > 0);

                return $result;
            },
        );
    }

    public function truncate(): void
    {
        $this->observeWrite(
            true,
            true,
            function (): void {
                parent::truncate();
                $this->recordTerminal(true, true);
            },
            forceBroad: true,
        );
    }

    private function mustBypassNormCache(): bool
    {
        return $this->normCacheOrigin === null
            || $this->normCacheSkipped
            || $this->connectionPretending()
            || $this->connection->transactionLevel() > 0
            || $this->useWritePdo
            || $this->lock !== null
            || $this->fetchUsing !== []
            || $this->hasCustomDefaultFetchMode()
            || $this->normCacheWriteDepth > 0;
    }

    private function runBypassedExists(string $sql, array $bindings): array
    {
        if (($reason = $this->bypassReason()) !== null) {
            app(Reporter::class)->bypass($this, $reason, $sql, $bindings);
        }

        return $this->connection->select($sql, $bindings, !$this->useWritePdo);
    }

    private function bypassReason(): ?string
    {
        if ($this->normCacheOrigin === null || $this->normCacheWriteDepth > 0) {
            return null;
        }

        return match (true) {
            $this->normCacheSkipped => 'explicit_without_cache',
            $this->connectionPretending() => 'connection_pretending',
            $this->connection->transactionLevel() > 0 => 'transaction_active',
            $this->useWritePdo => 'write_pdo',
            $this->lock !== null => 'locking_read',
            $this->fetchUsing !== [] => 'custom_fetch_mode',
            $this->hasCustomDefaultFetchMode() => 'custom_fetch_mode',
            default => null,
        };
    }

    private function observeWrite(
        bool $broad,
        bool $forceWhenExecuted,
        callable $operation,
        bool $forceBroad = false,
        ?array $assigned = null,
    ): mixed {
        $owner = $this->normCacheWriteDepth === 0;

        if ($owner) {
            $this->normCacheTerminalExecuted = false;
            $this->normCacheWriteChanged = false;
            $this->normCacheWriteForcedBroad = false;
            $this->normCacheWriteAssignments = null;
        }

        $this->normCacheWriteForcedBroad = $this->normCacheWriteForcedBroad || $forceBroad;

        if ($assigned !== null) {
            $this->normCacheWriteAssignments = $assigned;
        }

        $this->normCacheWriteDepth++;

        try {
            $result = $operation();
        } finally {
            $this->normCacheWriteDepth--;
        }

        if ($owner) {
            $this->finishWriteObservation(
                $broad,
                $forceWhenExecuted,
                $this->normCacheTerminalExecuted,
                $this->normCacheWriteChanged,
                $this->normCacheWriteForcedBroad,
                $this->normCacheWriteAssignments,
            );
        }

        return $result;
    }

    protected function recordTerminal(bool $executed, bool $changed): void
    {
        $this->normCacheTerminalExecuted = $this->normCacheTerminalExecuted || $executed;
        $this->normCacheWriteChanged = $this->normCacheWriteChanged || $changed;
    }

    /** @param array<string, mixed>|null $assignments */
    protected function finishWriteObservation(
        bool $mayAffectRows,
        bool $forceWhenExecuted,
        bool $executed,
        bool $changed,
        bool $forceBroad,
        ?array $assignments,
    ): void {
        if (!$executed || !$forceWhenExecuted && !$changed) {
            return;
        }

        app(Invalidator::class)->afterWrite(
            $this,
            $mayAffectRows,
            $forceBroad,
            $assignments,
        );
    }

    private function connectionPretending(): bool
    {
        return $this->connection instanceof Connection
            && $this->connection->pretending();
    }

    private function hasCustomDefaultFetchMode(): bool
    {
        if (!$this->connection instanceof Connection) {
            return true;
        }

        $options = (array) $this->connection->getConfig('options');
        $mode = $options[PDO::ATTR_DEFAULT_FETCH_MODE] ?? PDO::FETCH_OBJ;

        return $mode !== PDO::FETCH_OBJ;
    }
}
