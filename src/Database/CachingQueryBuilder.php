<?php

namespace NormCache\Database;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Query\Processors\Processor;
use InvalidArgumentException;
use NormCache\Cache\Engine;
use NormCache\Invalidator;
use NormCache\Support\QueryIdentity;
use NormCache\Support\Reporter;
use NormCache\Values\DependencyDeclaration;
use NormCache\Values\PrimaryKeyMetadata;
use PDO;

final class CachingQueryBuilder extends Builder
{
    public const ORIGIN_CACHEABLE_MODEL = 'cacheable_model';

    public const ORIGIN_DB_TABLE = 'db_table';

    // Declared explicitly: Laravel 12's Query\Builder doesn't define fetchUsing()/$fetchUsing
    // (added in 13), but this class is used under both ^12.0 and ^13.0 per composer.json.
    public array $fetchUsing = [];

    private Connection $normCacheConnection;

    private ?string $normCacheOrigin = null;

    /** @var class-string|null */
    private ?string $normCacheModelClass = null;

    private ?PrimaryKeyMetadata $normCachePrimaryKey = null;

    private ?string $normCacheDeletedAtColumn = null;

    private bool $normCacheSkipped = false;

    private ?int $normCacheTtl = null;

    private ?string $normCacheTag = null;

    private bool $normCacheUseResultCache = false;

    /** @var array<string, DependencyDeclaration> */
    private array $normCacheDependencies = [];

    private int $normCacheWriteDepth = 0;

    private bool $normCacheTerminalExecuted = false;

    private bool $normCacheWriteChanged = false;

    private bool $normCacheWriteForcedBroad = false;

    /** @var array<string, mixed>|null */
    private ?array $normCacheWriteAssignments = null;

    public function __construct(
        Connection $connection,
        ?Grammar $grammar = null,
        ?Processor $processor = null,
    ) {
        parent::__construct($connection, $grammar, $processor);
        $this->normCacheConnection = $connection;
    }

    public function getConnection(): Connection
    {
        return $this->normCacheConnection;
    }

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

    public function useResultCache(): static
    {
        $this->normCacheUseResultCache = true;

        return $this;
    }

    public function usesNormCacheResultCache(): bool
    {
        return $this->normCacheUseResultCache;
    }

    /** @param array<mixed> $dependencies */
    public function dependsOn(array $dependencies): static
    {
        if ($dependencies === []) {
            throw new InvalidArgumentException(
                'dependsOn() requires at least one model class or table name.'
            );
        }

        foreach ($dependencies as $dependency) {
            if (!is_string($dependency)) {
                throw new InvalidArgumentException(
                    'dependsOn() expects model class names or table names.'
                );
            }

            if (
                class_exists($dependency)
                || interface_exists($dependency)
                || trait_exists($dependency)
                || enum_exists($dependency)
            ) {
                if (!is_a($dependency, Model::class, true)) {
                    throw new InvalidArgumentException(
                        "dependsOn() class [{$dependency}] must be an Eloquent model."
                    );
                }

                $declaration = DependencyDeclaration::model($dependency);
            } else {
                $table = trim($dependency);

                if (str_contains($table, '\\')) {
                    throw new InvalidArgumentException(
                        "dependsOn() model class [{$dependency}] does not exist."
                    );
                }

                if ($table === '' || preg_match('/[:{}\s*]/', $table) === 1) {
                    throw new InvalidArgumentException(
                        'dependsOn() table names must not contain reserved characters (: { } * or whitespace).'
                    );
                }

                $declaration = DependencyDeclaration::table($table);
            }

            $this->normCacheDependencies[$declaration->key()] = $declaration;
        }

        return $this;
    }

    /** @return list<DependencyDeclaration> */
    public function normCacheDependencies(): array
    {
        return array_values($this->normCacheDependencies);
    }

    protected function runSelect()
    {
        $sql = $this->toSql();
        $bindings = $this->getBindings();

        [$bypass, $reason] = $this->bypassDecision();

        if ($bypass) {
            if ($reason !== null) {
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

        [$bypass, $reason] = $this->bypassDecision();
        $results = $bypass
            ? $this->runBypassedExists($sql, $bindings, $reason)
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
            mayAffectRows: false,
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

    public function insertGetId(array $values, $sequence = null): int|string
    {
        return $this->observeWrite(
            false,
            true,
            function () use ($values, $sequence) {
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

    /** @return array{0: bool, 1: ?string} */
    private function bypassDecision(): array
    {
        if ($this->normCacheOrigin === null || $this->normCacheWriteDepth > 0) {
            return [true, null];
        }

        $reason = match (true) {
            $this->normCacheSkipped => 'explicit_without_cache',
            $this->connectionPretending() => 'connection_pretending',
            $this->connection->transactionLevel() > 0 => 'transaction_active',
            $this->useWritePdo => 'write_pdo',
            $this->fetchUsing !== [] => 'custom_fetch_mode',
            $this->hasCustomDefaultFetchMode() => 'custom_fetch_mode',
            default => null,
        };

        return [$reason !== null, $reason];
    }

    private function runBypassedExists(
        string $sql,
        array $bindings,
        ?string $reason,
    ): array {
        if ($reason !== null) {
            app(Reporter::class)->bypass($this, $reason, $sql, $bindings);
        }

        return $this->connection->select($sql, $bindings, !$this->useWritePdo);
    }

    private function observeWrite(
        bool $mayAffectRows,
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
                $mayAffectRows,
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
        return $this->normCacheConnection->pretending();
    }

    private function hasCustomDefaultFetchMode(): bool
    {
        $options = (array) $this->normCacheConnection->getConfig('options');
        $mode = $options[PDO::ATTR_DEFAULT_FETCH_MODE] ?? PDO::FETCH_OBJ;

        return $mode !== PDO::FETCH_OBJ;
    }
}
