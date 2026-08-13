<?php

namespace NormCache\Database;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
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

    /** @var class-string|null */
    private ?string $modelClass = null;

    private ?PrimaryKeyMetadata $primaryKey = null;

    private ?string $deletedAtColumn = null;

    /** @var list<string> */
    private array $volatileColumns = [];

    private bool $skipped = false;

    private bool $internal = false;

    private ?int $ttl = null;

    private ?string $tag = null;

    private ?string $cacheContext = null;

    /** @var array<string, DependencyDeclaration> */
    private array $dependencies = [];

    /** @var \WeakMap<Expression, array{builder: Builder, sql: string}>|null */
    private ?\WeakMap $capturedSubqueries = null;

    private int $writeDepth = 0;

    private int $nestedMutationSequence = 0;

    public function __construct(
        Connection $connection,
        ?Grammar $grammar = null,
        ?Processor $processor = null,
    ) {
        parent::__construct($connection, $grammar, $processor);
    }

    public function selectSub($query, $as)
    {
        $result = parent::selectSub($query, $as);
        $subquery = $this->baseSubquery($query);

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
        $result = parent::joinSub(
            $query,
            $as,
            $first,
            $operator,
            $second,
            $type,
            $where,
        );
        $subquery = $this->baseSubquery($query);

        if ($subquery instanceof Builder) {
            $join = end($this->joins);
            $produced = $join->table;

            if ($produced instanceof Expression) {
                $this->captureSubquery($produced, $subquery);
            }
        }

        return $result;
    }

    private function baseSubquery(mixed $query): ?Builder
    {
        if ($query instanceof EloquentBuilder || $query instanceof Relation) {
            return $query->toBase();
        }

        return $query instanceof Builder ? $query : null;
    }

    private function captureSubquery(Expression $expression, Builder $subquery): void
    {
        $this->capturedSubqueries ??= new \WeakMap;
        $this->capturedSubqueries[$expression] = [
            'builder' => $subquery,
            'sql' => $subquery->getGrammar()->compileSelect($subquery),
        ];
    }

    public function capturedSubquery(Expression $expression): ?Builder
    {
        $captured = $this->capturedSubqueries === null
            ? null
            : ($this->capturedSubqueries[$expression] ?? null);

        if ($captured !== null && $this->capturedBuilderIsCurrent($captured)) {
            return $captured['builder'];
        }

        return null;
    }

    /** @param array{builder: Builder, sql: string} $captured */
    private function capturedBuilderIsCurrent(array $captured): bool
    {
        try {
            return $captured['builder']->getGrammar()->compileSelect($captured['builder'])
                === $captured['sql'];
        } catch (\Throwable) {
            return false;
        }
    }

    public function getConnection(): Connection
    {
        // The constructor narrows the inherited property to Connection.
        /** @var Connection */
        return $this->connection;
    }

    /** @param class-string $modelClass */
    public function enableCachingForModel(
        string $modelClass,
        string $keyName,
        string $keyType,
        ?string $deletedAtColumn = null,
        array $volatileColumns = [],
    ): static {
        $this->modelClass = $modelClass;
        $this->primaryKey = new PrimaryKeyMetadata(
            $keyName,
            $keyType === 'int' || $keyType === 'integer'
                ? PrimaryKeyMetadata::INTEGER
                : PrimaryKeyMetadata::STRING,
        );
        $this->deletedAtColumn = $deletedAtColumn;
        $this->volatileColumns = $volatileColumns;

        return $this;
    }

    /** @return list<string> */
    public function volatileColumns(): array
    {
        return $this->volatileColumns;
    }

    /** @return class-string|null */
    public function modelClass(): ?string
    {
        return $this->modelClass;
    }

    public function primaryKey(): ?PrimaryKeyMetadata
    {
        return $this->primaryKey;
    }

    public function deletedAtColumn(): ?string
    {
        return $this->deletedAtColumn;
    }

    public function withoutCache(): static
    {
        $this->skipped = true;

        return $this;
    }

    public function internal(): static
    {
        $this->internal = true;

        return $this;
    }

    public function isInternal(): bool
    {
        return $this->internal;
    }

    public function ttl(int $seconds): static
    {
        if ($seconds < 1) {
            throw new \InvalidArgumentException('NormCache TTL must be greater than zero.');
        }

        $this->ttl = $seconds;

        return $this;
    }

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

    /** @return list<DependencyDeclaration> */
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

        // The bypass guard rejects write PDOs and custom fetch modes.
        $select = fn(bool $useReadPdo): array => $this->connection->select(
            $statement->sql(),
            $statement->bindings(),
            $useReadPdo,
        );

        // Direct primary-key hits never need compiled SQL.
        return app(Engine::class)->select(
            $this,
            $statement,
            'select',
            fn() => $select(true),
            fn() => $select(false),
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
                fn() => $this->connection->select($statement->sql(), $statement->bindings(), false),
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
            operation: fn(): bool => parent::insert($values),
            invalidate: static fn(bool $result): bool => $values !== [] && $result,
        );
    }

    public function insertOrIgnore(array $values): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            operation: fn(): int => parent::insertOrIgnore($values),
            invalidate: static fn(int $result): bool => $result > 0,
        );
    }

    public function insertOrIgnoreReturning(array $values, array $returning = ['*'], $uniqueBy = null): mixed
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            operation: fn(): mixed => parent::insertOrIgnoreReturning($values, $returning, $uniqueBy),
            invalidate: static fn($result): bool => $result->isNotEmpty(),
        );
    }

    public function insertGetId(array $values, $sequence = null): int|string
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            operation: fn() => parent::insertGetId($values, $sequence),
        );
    }

    public function insertUsing(array $columns, $query): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            operation: fn(): int => parent::insertUsing($columns, $query),
            invalidate: static fn(int $result): bool => $result > 0,
        );
    }

    public function insertOrIgnoreUsing(array $columns, $query): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::INSERT,
            mayAffectExistingRows: false,
            operation: fn(): int => parent::insertOrIgnoreUsing($columns, $query),
            invalidate: static fn(int $result): bool => $result > 0,
        );
    }

    public function update(array $values): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPDATE,
            mayAffectExistingRows: true,
            operation: fn(): int => parent::update($values),
            invalidate: static fn(int $result): bool => $result > 0,
            assigned: $values,
        );
    }

    public function updateFrom(array $values): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPDATE,
            mayAffectExistingRows: true,
            operation: fn(): int => parent::updateFrom($values),
            invalidate: static fn(int $result): bool => $result > 0,
            assigned: $values,
        );
    }

    public function updateOrInsert(array $attributes, $values = []): bool
    {
        $mutationSequence = $this->nestedMutationSequence;

        return $this->writeWithInvalidation(
            mutation: MutationType::UPSERT,
            mayAffectExistingRows: true,
            operation: fn(): bool => parent::updateOrInsert($attributes, $values),
            invalidate: fn(): bool => $this->nestedMutationSequence !== $mutationSequence,
            forceBroadInvalidation: true,
        );
    }

    public function upsert(array $values, $uniqueBy, $update = null): int
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::UPSERT,
            mayAffectExistingRows: true,
            operation: fn(): int => parent::upsert($values, $uniqueBy, $update),
            invalidate: $values !== [],
            forceBroadInvalidation: true,
        );
    }

    public function delete($id = null)
    {
        return $this->writeWithInvalidation(
            mutation: MutationType::DELETE,
            mayAffectExistingRows: true,
            operation: fn() => parent::delete($id),
            invalidate: static fn(int $result): bool => $result > 0,
        );
    }

    public function truncate(): void
    {
        $this->writeWithInvalidation(
            mutation: MutationType::TRUNCATE,
            mayAffectExistingRows: true,
            operation: fn() => parent::truncate(),
            forceBroadInvalidation: true,
        );
    }

    /** @return array{0: bool, 1: ?string} */
    private function bypassDecision(): array
    {
        if ($this->writeDepth > 0) {
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

    /**
     * $invalidate receives the operation's return value; callables deciding from
     * builder state instead may declare no parameters.
     *
     * @param  bool|callable(mixed): bool  $invalidate
     */
    private function writeWithInvalidation(
        MutationType $mutation,
        bool $mayAffectExistingRows,
        callable $operation,
        bool|callable $invalidate = true,
        bool $forceBroadInvalidation = false,
        ?array $assigned = null,
    ): mixed {
        $owner = $this->writeDepth === 0;

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

        $shouldInvalidate = is_bool($invalidate) ? $invalidate : $invalidate($result);

        if (!$owner) {
            if ($shouldInvalidate) {
                $this->nestedMutationSequence++;
            }

            return $result;
        }

        if ($shouldInvalidate) {
            app(Invalidator::class)->afterWrite(
                query: $this,
                mutation: $mutation,
                mayAffectExistingRows: $mayAffectExistingRows,
                forceBroadInvalidation: $forceBroadInvalidation,
                assigned: $assigned,
            );
        }

        return $result;
    }

    private function connectionPretending(): bool
    {
        return $this->getConnection()->pretending();
    }

    private function hasCustomDefaultFetchMode(): bool
    {
        $options = (array) $this->getConnection()->getConfig('options');
        $mode = $options[\PDO::ATTR_DEFAULT_FETCH_MODE] ?? \PDO::FETCH_OBJ;

        return $mode !== \PDO::FETCH_OBJ;
    }
}
