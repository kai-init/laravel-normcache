<?php

namespace NormCache\Database;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Query\JoinClause;
use Illuminate\Database\Query\Processors\Processor;
use NormCache\Cache\Engine;
use NormCache\Enums\MutationType;
use NormCache\Invalidator;
use NormCache\Support\FailureReporter;
use NormCache\Support\QueryIdentity;
use NormCache\Support\QueryObserver;
use NormCache\Values\DependencyDeclaration;
use NormCache\Values\PrimaryKeyMetadata;

final class QueryBuilder extends Builder
{
    public array $fetchUsing = [];

    private Connection $databaseConnection;

    private bool $eligible = false;

    /** @var class-string|null */
    private ?string $modelClass = null;

    private ?PrimaryKeyMetadata $primaryKey = null;

    private ?string $deletedAtColumn = null;

    private bool $skipped = false;

    private ?int $ttl = null;

    private ?string $tag = null;

    private ?string $cacheContext = null;

    /** @var array<string, DependencyDeclaration> */
    private array $dependencies = [];

    /** @var \WeakMap<Expression, array{builder: Builder, sql: string}>|null */
    private ?\WeakMap $capturedSubqueries = null;

    private int $writeDepth = 0;

    private bool $writeChanged = false;

    private bool $writeExecuted = false;

    public function __construct(
        Connection $connection,
        ?Grammar $grammar = null,
        ?Processor $processor = null,
    ) {
        parent::__construct($connection, $grammar, $processor);
        $this->databaseConnection = $connection;
    }

    public function selectSub($query, $as)
    {
        $subquery = $query instanceof EloquentBuilder ? $query->toBase() : $query;
        $result = parent::selectSub($query, $as);

        if ($subquery instanceof Builder) {
            $produced = end($this->columns);

            if ($produced instanceof Expression) {
                $this->captureSubquery($produced, $subquery);
            }
        }

        return $result;
    }

    public function joinSub(
        $query,
        $as,
        $first,
        $operator = null,
        $second = null,
        $type = 'inner',
        $where = false,
    ) {
        $subquery = $query instanceof EloquentBuilder ? $query->toBase() : $query;
        $result = parent::joinSub(
            $query,
            $as,
            $first,
            $operator,
            $second,
            $type,
            $where,
        );

        if ($subquery instanceof Builder) {
            $join = end($this->joins);
            $produced = $join instanceof JoinClause ? $join->table : null;

            if ($produced instanceof Expression) {
                $this->captureSubquery($produced, $subquery);
            }
        }

        return $result;
    }

    private function captureSubquery(Expression $expression, Builder $subquery): void
    {
        $this->capturedSubqueries ??= new \WeakMap;
        $this->capturedSubqueries[$expression] = [
            'builder' => $subquery,
            'sql' => $subquery->getGrammar()->compileSelect($subquery),
        ];
    }

    /** @internal */
    public function capturedSubquery(Expression $expression): ?Builder
    {
        $captured = $this->capturedSubqueries === null
            ? null
            : ($this->capturedSubqueries[$expression] ?? null);

        if ($captured === null) {
            return null;
        }

        try {
            $sql = $captured['builder']->getGrammar()->compileSelect($captured['builder']);
        } catch (\Throwable) {
            return null;
        }

        return $sql === $captured['sql'] ? $captured['builder'] : null;
    }

    public function getConnection(): Connection
    {
        return $this->databaseConnection;
    }

    /** @internal */
    public function enableCachingForTable(): static
    {
        $this->eligible = true;

        return $this;
    }

    /**
     * @internal
     *
     * @param  class-string  $modelClass
     */
    public function enableCachingForModel(
        string $modelClass,
        string $keyName,
        string $keyType,
        ?string $deletedAtColumn = null,
    ): static {
        $this->eligible = true;
        $this->modelClass = $modelClass;
        $this->primaryKey = new PrimaryKeyMetadata(
            $keyName,
            $keyType === 'int' || $keyType === 'integer'
                ? PrimaryKeyMetadata::INTEGER
                : PrimaryKeyMetadata::STRING,
        );
        $this->deletedAtColumn = $deletedAtColumn;

        return $this;
    }

    /**
     * @internal
     *
     * @return class-string|null
     */
    public function modelClass(): ?string
    {
        return $this->modelClass;
    }

    /** @internal */
    public function primaryKey(): ?PrimaryKeyMetadata
    {
        return $this->primaryKey;
    }

    /** @internal */
    public function deletedAtColumn(): ?string
    {
        return $this->deletedAtColumn;
    }

    public function withoutCache(): static
    {
        $this->skipped = true;

        return $this;
    }

    public function ttl(int $seconds): static
    {
        if ($seconds < 1) {
            throw new \InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->ttl = $seconds;

        return $this;
    }

    /** @internal */
    public function configuredTtl(): ?int
    {
        return $this->ttl;
    }

    public function tag(string $tag): static
    {
        (new QueryIdentity)->tagHash($tag);
        $this->tag = $tag;

        return $this;
    }

    /** @internal */
    public function configuredTag(): ?string
    {
        return $this->tag;
    }

    public function cacheContext(string $context): static
    {
        (new QueryIdentity)->contextHash($context);
        $this->cacheContext = $context;

        return $this;
    }

    /** @internal */
    public function configuredCacheContext(): ?string
    {
        return $this->cacheContext;
    }

    /** @param array<mixed> $dependencies */
    public function dependsOn(array $dependencies): static
    {
        if ($dependencies === []) {
            throw new \InvalidArgumentException(
                'dependsOn() requires at least one model class or table name.'
            );
        }

        foreach ($dependencies as $dependency) {
            $declaration = $this->dependencyDeclaration($dependency);
            $this->dependencies[$declaration->key()] = $declaration;
        }

        return $this;
    }

    private function dependencyDeclaration(mixed $dependency): DependencyDeclaration
    {
        if (!is_string($dependency)) {
            throw new \InvalidArgumentException(
                'dependsOn() expects model class names or table names.'
            );
        }

        if (is_a($dependency, Model::class, true)) {
            return DependencyDeclaration::model($dependency);
        }

        if ($this->isDefinedType($dependency)) {
            throw new \InvalidArgumentException(
                "dependsOn() class [{$dependency}] must be an Eloquent model."
            );
        }

        $table = trim($dependency);

        if (str_contains($table, '\\')) {
            throw new \InvalidArgumentException(
                "dependsOn() model class [{$dependency}] does not exist."
            );
        }

        if ($table === '' || preg_match('/[:{}\s*]/', $table) === 1) {
            throw new \InvalidArgumentException(
                'dependsOn() table names must not contain reserved characters (: { } * or whitespace).'
            );
        }

        return DependencyDeclaration::table($table);
    }

    private function isDefinedType(string $type): bool
    {
        return class_exists($type)
            || interface_exists($type)
            || trait_exists($type)
            || enum_exists($type);
    }

    /**
     * @internal
     *
     * @return list<DependencyDeclaration>
     */
    public function dependencies(): array
    {
        return array_values($this->dependencies);
    }

    protected function runSelect()
    {
        $this->applyBeforeQueryCallbacks();
        $statement = new QueryStatement(fn(): array => [$this->toSql(), $this->getBindings()]);

        [$bypass, $reason] = $this->bypassDecision();

        if ($bypass) {
            if ($reason !== null) {
                app(QueryObserver::class)->bypass($this, $reason, $statement);
            }

            return $this->connection->select(
                $statement->sql(),
                $statement->bindings(),
                !$this->useWritePdo,
                $this->fetchUsing,
            );
        }

        // Compiling SQL is deferred: a direct primary-key hit resolves its row
        // from the plan alone and never reads the statement or its hash.
        return app(Engine::class)->select(
            $this,
            $statement,
            'select',
            fn() => $this->connection->select(
                $statement->sql(),
                $statement->bindings(),
                !$this->useWritePdo,
                $this->fetchUsing,
            ),
            fn() => $this->connection->select($statement->sql(), $statement->bindings(), false, []),
        );
    }

    public function exists()
    {
        $this->applyBeforeQueryCallbacks();
        $sql = $this->grammar->compileExists($this);
        $bindings = $this->getBindings();
        $statement = new QueryStatement(fn(): array => [$sql, $bindings]);

        [$bypass, $reason] = $this->bypassDecision();
        $results = $bypass
            ? $this->runBypassedExists($statement, $reason)
            : app(Engine::class)->select(
                $this,
                $statement,
                'exists',
                fn() => $this->connection->select($statement->sql(), $statement->bindings(), true),
                fn() => $this->connection->select($statement->sql(), $statement->bindings(), false, []),
            );

        if (!isset($results[0])) {
            return false;
        }

        $result = (array) $results[0];

        return (bool) $result['exists'];
    }

    public function insert(array $values): bool
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            forceInvalidation: false,
            operation: function () use ($values): bool {
                $result = parent::insert($values);
                $this->recordOutcome($values !== [], $values !== [] && $result);

                return $result;
            },
        );
    }

    public function insertOrIgnore(array $values): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            forceInvalidation: false,
            operation: function () use ($values): int {
                $result = parent::insertOrIgnore($values);
                $this->recordOutcome($values !== [], $result > 0);

                return $result;
            },
        );
    }

    public function insertOrIgnoreReturning(array $values, array $returning = ['*'], $uniqueBy = null): mixed
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            forceInvalidation: false,
            operation: function () use ($values, $returning, $uniqueBy): mixed {
                $result = parent::insertOrIgnoreReturning($values, $returning, $uniqueBy);
                $this->recordOutcome($values !== [], $result->isNotEmpty());

                return $result;
            },
        );
    }

    public function insertGetId(array $values, $sequence = null): int|string
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            forceInvalidation: true,
            operation: function () use ($values, $sequence) {
                $result = parent::insertGetId($values, $sequence);
                $this->recordOutcome(true, true);

                return $result;
            },
        );
    }

    public function insertUsing(array $columns, $query): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            forceInvalidation: false,
            operation: function () use ($columns, $query): int {
                $result = parent::insertUsing($columns, $query);
                $this->recordOutcome(true, $result > 0);

                return $result;
            },
        );
    }

    public function insertOrIgnoreUsing(array $columns, $query): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            forceInvalidation: false,
            operation: function () use ($columns, $query): int {
                $result = parent::insertOrIgnoreUsing($columns, $query);
                $this->recordOutcome(true, $result > 0);

                return $result;
            },
        );
    }

    public function update(array $values): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPDATE,
            mayAffectExistingRows: true,
            forceInvalidation: false,
            operation: function () use ($values): int {
                $result = parent::update($values);
                $this->recordOutcome(true, $result > 0);

                return $result;
            },
            assigned: $values,
        );
    }

    public function updateFrom(array $values): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPDATE,
            mayAffectExistingRows: true,
            forceInvalidation: false,
            operation: function () use ($values): int {
                $result = parent::updateFrom($values);
                $this->recordOutcome(true, $result > 0);

                return $result;
            },
            assigned: $values,
        );
    }

    public function updateOrInsert(array $attributes, $values = []): bool
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPSERT,
            mayAffectExistingRows: true,
            forceInvalidation: true,
            operation: fn(): bool => parent::updateOrInsert($attributes, $values),
            forceBroadInvalidation: true,
        );
    }

    public function upsert(array $values, $uniqueBy, $update = null): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPSERT,
            mayAffectExistingRows: true,
            forceInvalidation: true,
            operation: function () use ($values, $uniqueBy, $update): int {
                $result = parent::upsert($values, $uniqueBy, $update);

                if ($values !== []) {
                    $this->recordOutcome(true, $result > 0);
                }

                return $result;
            },
            forceBroadInvalidation: true,
        );
    }

    public function delete($id = null)
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::DELETE,
            mayAffectExistingRows: true,
            forceInvalidation: false,
            operation: function () use ($id) {
                $result = parent::delete($id);
                $this->recordOutcome(true, $result > 0);

                return $result;
            },
        );
    }

    public function truncate(): void
    {
        $this->writeWithInvalidation(
            mutation: MutationType::TRUNCATE,
            mayAffectExistingRows: true,
            forceInvalidation: true,
            operation: function (): void {
                parent::truncate();
                $this->recordOutcome(true, true);
            },
            forceBroadInvalidation: true,
        );
    }

    /** @return array{0: bool, 1: ?string} */
    private function bypassDecision(): array
    {
        if (!$this->eligible || $this->writeDepth > 0) {
            return [true, null];
        }

        $reason = match (true) {
            $this->skipped => 'explicit_without_cache',
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
        QueryStatement $statement,
        ?string $reason,
    ): array {
        if ($reason !== null) {
            app(QueryObserver::class)->bypass($this, $reason, $statement);
        }

        return $this->connection->select(
            $statement->sql(),
            $statement->bindings(),
            !$this->useWritePdo,
        );
    }

    private function writeWithInvalidation(
        MutationType $mutation,
        bool $mayAffectExistingRows,
        bool $forceInvalidation,
        callable $operation,
        bool $forceBroadInvalidation = false,
        ?array $assigned = null,
    ): mixed {
        $owner = $this->writeDepth === 0;

        if ($owner) {
            $this->writeExecuted = false;
            $this->writeChanged = false;
        }

        $this->writeDepth++;
        $failure = null;
        $result = null;

        try {
            $result = $operation();
        } catch (\Throwable $exception) {
            $failure = $exception;
        } finally {
            $this->writeDepth--;
        }

        if ($failure !== null) {
            if ($owner) {
                try {
                    app(Invalidator::class)->afterWrite(
                        query: $this,
                        mutation: $mutation,
                        mayAffectExistingRows: $mayAffectExistingRows,
                        forceBroadInvalidation: true,
                        assigned: $assigned,
                    );
                } catch (\Throwable $exception) {
                    app(FailureReporter::class)->cacheUnavailable($exception);
                }
            }

            throw $failure;
        }

        if ($owner) {
            $this->finishWriteObservation(
                mutation: $mutation,
                mayAffectExistingRows: $mayAffectExistingRows,
                forceInvalidation: $forceInvalidation,
                executed: $this->writeExecuted,
                changed: $this->writeChanged,
                forceBroadInvalidation: $forceBroadInvalidation,
                assignments: $assigned,
            );
        }

        return $result;
    }

    protected function recordOutcome(bool $executed, bool $changed): void
    {
        $this->writeExecuted = $this->writeExecuted || $executed;
        $this->writeChanged = $this->writeChanged || $changed;
    }

    /** @param array<string, mixed>|null $assignments */
    protected function finishWriteObservation(
        MutationType $mutation,
        bool $mayAffectExistingRows,
        bool $forceInvalidation,
        bool $executed,
        bool $changed,
        bool $forceBroadInvalidation,
        ?array $assignments,
    ): void {
        if (!$executed || !$forceInvalidation && !$changed) {
            return;
        }

        app(Invalidator::class)->afterWrite(
            query: $this,
            mutation: $mutation,
            mayAffectExistingRows: $mayAffectExistingRows,
            forceBroadInvalidation: $forceBroadInvalidation,
            assigned: $assignments,
        );
    }

    private function connectionPretending(): bool
    {
        return $this->databaseConnection->pretending();
    }

    private function hasCustomDefaultFetchMode(): bool
    {
        $options = (array) $this->databaseConnection->getConfig('options');
        $mode = $options[\PDO::ATTR_DEFAULT_FETCH_MODE] ?? \PDO::FETCH_OBJ;

        return $mode !== \PDO::FETCH_OBJ;
    }
}
