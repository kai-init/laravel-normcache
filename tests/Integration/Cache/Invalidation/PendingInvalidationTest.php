<?php

namespace NormCache\Tests\Integration\Cache\Invalidation;

use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\Events\JobExceptionOccurred;
use Illuminate\Queue\Events\JobProcessed;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\CacheRuntime;
use NormCache\Cache\PendingInvalidations;
use NormCache\Facades\NormCache;
use NormCache\Invalidator;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class PendingInvalidationTest extends TestCase
{
    protected function defineEnvironment($app): void
    {
        parent::defineEnvironment($app);

        $app['config']->set('normcache.max_precise_invalidation_keys', 3);
    }

    protected function setUp(): void
    {
        parent::setUp();

        foreach (range(1, 5) as $id) {
            Author::query()->create(['id' => $id, 'name' => 'Before']);
        }
    }

    public function test_cumulative_overflow_discards_tokens_and_invalidates_on_commit(): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $generationKey = $this->cacheKeys()->generation($table);
        $before = $this->cacheStore()->getRaw($generationKey);
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::transaction(function () use ($generationKey, $before): void {
            Author::query()->whereIn('id', [1, 2, 3])->update(['name' => 'At limit']);
            Author::query()->whereKey(1)->update(['name' => 'After']);
            $this->assertCount(3, $this->pending()[0]['tokens']);
            $this->assertFalse($this->pending()[0]['broad']);

            Author::query()->whereKey(4)->update(['name' => 'Overflow']);
            $this->assertSame([], $this->pending()[0]['tokens']);
            $this->assertTrue($this->pending()[0]['broad']);

            Author::query()->whereKey(5)->update(['name' => 'Still broad']);
            $this->assertSame([], $this->pending()[0]['tokens']);
            $this->assertSame($before, $this->cacheStore()->getRaw($generationKey));
        });

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($generationKey));
        $this->assertSame('After', Author::query()->findOrFail(1)->name);
        $this->assertSame([], $this->pending());
    }

    public function test_broad_invalidation_discards_previously_queued_tokens(): void
    {
        DB::transaction(function (): void {
            Author::query()->whereKey(1)->update(['name' => 'After']);
            $this->assertCount(1, $this->pending()[0]['tokens']);

            NormCache::invalidate(Author::class);
            Author::query()->whereKey(2)->update(['name' => 'After']);

            $this->assertTrue($this->pending()[0]['broad']);
            $this->assertSame([], $this->pending()[0]['tokens']);
        });
    }

    public function test_global_invalidation_replaces_pending_table_invalidations(): void
    {
        $epochKey = $this->cacheKeys()->epoch();
        $before = $this->cacheStore()->getRaw($epochKey);
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::transaction(function () use ($epochKey, $before): void {
            Author::query()->whereKey(1)->update(['name' => 'After']);
            $this->assertCount(1, $this->pending());

            Author::query()->toBase()->fromRaw('authors /* opaque write target */')->where('id', 1)->update(['name' => 'Opaque']);
            $this->assertSame([], $this->pending());
            Author::query()->whereKey(2)->update(['name' => 'After']);
            $this->assertSame([], $this->pending());
            $this->assertSame($before, $this->cacheStore()->getRaw($epochKey));
        });

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($epochKey));
        $this->assertSame('Opaque', Author::query()->findOrFail(1)->name);
    }

    public function test_nested_savepoint_rollback_preserves_outer_invalidation_until_commit(): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $versionKey = $this->cacheKeys()->version($table);
        $before = $this->cacheStore()->getRaw($versionKey) ?? '0';
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::transaction(function () use ($versionKey, $before): void {
            Author::query()->whereKey(1)->update(['name' => 'Outer']);

            try {
                DB::transaction(function (): void {
                    Author::query()->whereKey(2)->update(['name' => 'Inner']);
                    throw new \RuntimeException('rollback savepoint');
                });
            } catch (\RuntimeException) {
            }

            $this->assertSame($before, $this->cacheStore()->getRaw($versionKey) ?? '0');
            $this->assertCount(2, $this->pending()[0]['tokens']);
        });

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($versionKey));
        $this->assertSame('Outer', Author::query()->findOrFail(1)->name);
        $this->assertSame('Before', Author::query()->findOrFail(2)->name);
        $this->assertSame([], $this->pending());
    }

    public function test_mysql_implicit_commit_drains_earlier_writes_before_rollback(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('Implicit commit requires MySQL');
        }

        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::beginTransaction();
        Author::query()->whereKey(1)->update(['name' => 'After']);

        // Truncate unrelated fixture table through Cacheable builder
        Post::query()->truncate();

        DB::rollBack();

        $this->assertSame('After', Author::query()->findOrFail(1)->name);
        $this->assertSame([], $this->pending());
    }

    public function test_subsequent_write_after_implicit_commit_is_invalidated_immediately(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('Implicit commit requires MySQL');
        }

        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        DB::beginTransaction();
        Post::query()->truncate();

        Author::query()->whereKey(1)->update(['name' => 'Subsequent']);

        DB::rollBack();

        $this->assertSame('Subsequent', Author::query()->findOrFail(1)->name);
        $this->assertSame([], $this->pending());
    }

    public function test_scope_reconciliation_isolated_across_named_connections(): void
    {
        $secondaryName = 'pending-secondary';
        $originalPdo = DB::connection('testing')->getPdo();
        $connectionConfig = config('database.connections.testing');
        config()->set("database.connections.{$secondaryName}", array_replace(
            $connectionConfig,
            [
                'name' => $secondaryName,
                'normcache_scope' => $secondaryName,
            ],
        ));

        $secondary = null;
        $secondaryPdo = null;

        try {
            $first = DB::connection('testing');
            $secondary = DB::connection($secondaryName);
            $firstPdo = $first->getPdo();
            $secondaryPdo = $secondary->getPdo();
            $firstTable = app(TableIdentityResolver::class)->resolve($first, 'authors');
            $secondaryTable = app(TableIdentityResolver::class)->resolve($secondary, 'authors');

            $first->beginTransaction();
            $secondary->beginTransaction();
            $old = app(Invalidator::class);
            $this->assertTrue($old->invalidateTables([$firstTable]));
            $this->assertTrue($old->invalidateTables([$secondaryTable]));
            $this->assertTrue($old->hasPending('testing'));
            $this->assertTrue($old->hasPending($secondaryName));

            $firstPdo->commit();
            $this->assertFalse($firstPdo->inTransaction());
            $this->assertSame(1, $first->transactionLevel());

            $this->app->forgetScopedInstances();
            NormCache::clearResolvedInstances();
            $new = app(Invalidator::class);
            $this->assertNotSame($old, $new);
            $this->assertFalse($new->hasPending('testing'));
            $this->assertTrue($new->hasPending($secondaryName));
            $this->assertTrue($secondaryPdo->inTransaction());

            $secondary->rollBack();
            $this->assertFalse($new->hasPending($secondaryName));
        } finally {
            if ($secondaryPdo instanceof \PDO && $secondaryPdo->inTransaction()) {
                $secondaryPdo->rollBack();
            }
            if (isset($firstPdo) && $firstPdo->inTransaction()) {
                $firstPdo->rollBack();
            }
            if (isset($first)) {
                $first->setPdo($originalPdo);
            }
            DB::disconnect($secondaryName);
            DB::purge($secondaryName);
        }
    }

    public function test_commit_then_throw_recovers_on_same_scope_read(): void
    {
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $this->withThrowingCommit(function (): void {
            try {
                DB::transaction(function (): void {
                    Author::query()->whereKey(1)->update(['name' => 'CommittedThenThrew']);
                });
                $this->fail('Expected commit to throw');
            } catch (\RuntimeException $exception) {
                $this->assertSame('Lost acknowledgement', $exception->getMessage());
            }

            $this->assertSame(0, DB::connection()->transactionLevel());
            $this->assertFalse(DB::connection()->getPdo()->inTransaction());
            $this->assertNotEmpty($this->pending());
            $this->assertSame('CommittedThenThrew', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'CommittedThenThrew');
        });
    }

    public function test_manual_commit_then_throw_is_reconciled_despite_logical_transaction_level(): void
    {
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $this->withThrowingCommit(function (\PDO $pdo, \PDO $original): void {
            DB::beginTransaction();
            Author::query()->whereKey(1)->update(['name' => 'ManualCommit']);
            try {
                DB::commit();
                $this->fail('Expected commit to throw');
            } catch (\RuntimeException $exception) {
                $this->assertSame('Lost acknowledgement', $exception->getMessage());
            }
            $this->assertSame(1, DB::connection()->transactionLevel());
            $this->assertFalse($pdo->inTransaction());
            $this->assertNotEmpty($this->pending());
            $this->assertSame('ManualCommit', Author::query()->findOrFail(1)->name);
            $this->assertSame([], $this->pending());
            DB::connection()->setPdo($original);
            $this->assertSame('ManualCommit', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'ManualCommit');
        });
    }

    public function test_rollback_after_committed_acknowledgement_failure_preserves_invalidation(): void
    {
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);

        $this->withThrowingCommit(function (\PDO $pdo): void {
            DB::beginTransaction();
            Author::query()->whereKey(1)->update(['name' => 'CommittedThenThrew']);

            try {
                DB::commit();
                $this->fail('Expected commit to throw');
            } catch (\RuntimeException $exception) {
                $this->assertSame('Lost acknowledgement', $exception->getMessage());
            }

            $this->assertSame(1, DB::connection()->transactionLevel());
            $this->assertFalse($pdo->inTransaction());
            DB::rollBack();
            $this->assertSame([], $this->pending());

            $this->assertSame('CommittedThenThrew', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'CommittedThenThrew');
        });
    }

    public function test_next_transaction_reconciles_before_begin_without_intervening_read(): void
    {
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $this->withThrowingCommit(function (\PDO $pdo): void {
            try {
                DB::transaction(function (): void {
                    Author::query()->whereKey(1)->update(['name' => 'CommittedBeforeNext']);
                });
                $this->fail('Expected commit to throw');
            } catch (\RuntimeException) {
            }
            $this->assertNotEmpty($this->pending());
            $this->assertSame($pdo, DB::connection()->getPdo());
            DB::beginTransaction();
            $this->assertTrue($pdo->inTransaction());
            $this->assertSame([], $this->pending());
            Author::query()->whereKey(2)->update(['name' => 'RolledBack']);
            DB::rollBack();
            $this->assertSame('CommittedBeforeNext', Author::query()->findOrFail(1)->name);
            $this->assertSame('Before', Author::query()->findOrFail(2)->name);
            $this->assertWarmFind(1, 'CommittedBeforeNext');
            $this->assertWarmFind(2, 'Before');
        });
    }

    public function test_native_deferred_foreign_key_commit_failure_retains_pending_until_rollback(): void
    {
        if (DB::connection()->getDriverName() !== 'sqlite') {
            $this->markTestSkipped('Deferred foreign key commit behavior is SQLite-specific');
        }

        $connection = DB::connection();
        $pdo = $connection->getPdo();
        $pdo->exec('PRAGMA foreign_keys = ON');
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        [$rowKey, $rawPayload] = $this->warmRowPayload(1);

        try {
            try {
                DB::transaction(function (): void {
                    DB::statement('PRAGMA defer_foreign_keys = ON');
                    Author::query()->whereKey(1)->update([
                        'name' => 'Uncommitted',
                        'country_id' => 987654,
                    ]);
                });
                $this->fail('Expected deferred foreign key commit to fail');
            } catch (\PDOException) {
            }

            $this->assertSame(0, $connection->transactionLevel());
            $this->assertTrue($pdo->inTransaction());
            $this->assertNotEmpty($this->pending());
            DB::flushQueryLog();
            DB::enableQueryLog();
            try {
                $this->assertSame('Uncommitted', Author::query()->findOrFail(1)->name);
                $this->assertCount(1, DB::getQueryLog());
            } finally {
                DB::disableQueryLog();
            }
            $this->assertSame($rawPayload, $this->cacheStore()->getRaw($rowKey));
            $this->assertNotEmpty($this->pending());

            $pdo->rollBack();
            $this->assertSame('Before', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'Before');
        } finally {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            $pdo->exec('PRAGMA defer_foreign_keys = OFF');
            $pdo->exec('PRAGMA foreign_keys = OFF');
        }
    }

    public function test_scope_reset_reconciles_shared_pending_with_new_invalidator(): void
    {
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $old = app(Invalidator::class);
        $this->withThrowingCommit(function () use ($old): void {
            try {
                DB::transaction(fn() => Author::query()->whereKey(1)->update(['name' => 'ScopeReset']));
                $this->fail('Expected commit to throw');
            } catch (\RuntimeException) {
            }
            $this->app->forgetScopedInstances();
            NormCache::clearResolvedInstances();
            $new = app(Invalidator::class);
            $this->assertNotSame($old, $new);
            $this->assertSame([], $this->pending());
            $this->assertSame('ScopeReset', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'ScopeReset');
        });
    }

    public function test_scope_reset_preserves_obligation_while_physical_transaction_active(): void
    {
        if (DB::connection()->getDriverName() !== 'sqlite') {
            $this->markTestSkipped('Active failed commit fixture is SQLite-specific');
        }

        $connection = DB::connection();
        $pdo = $connection->getPdo();
        $pdo->exec('PRAGMA foreign_keys = ON');
        $old = app(Invalidator::class);
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        [$rowKey, $rawPayload] = $this->warmRowPayload(1);

        try {
            try {
                DB::transaction(function (): void {
                    DB::statement('PRAGMA defer_foreign_keys = ON');
                    Author::query()->whereKey(1)->update([
                        'name' => 'ActiveUncommitted',
                        'country_id' => 987654,
                    ]);
                });
                $this->fail('Expected deferred foreign key commit to fail');
            } catch (\PDOException) {
            }

            $this->assertSame(0, $connection->transactionLevel());
            $this->assertTrue($pdo->inTransaction());
            $this->app->forgetScopedInstances();
            NormCache::clearResolvedInstances();
            $new = app(Invalidator::class);
            $this->assertNotSame($old, $new);
            $this->assertNotEmpty($this->pending());
            DB::flushQueryLog();
            DB::enableQueryLog();
            try {
                $this->assertSame('ActiveUncommitted', Author::query()->findOrFail(1)->name);
                $this->assertCount(1, DB::getQueryLog());
            } finally {
                DB::disableQueryLog();
            }
            $this->assertSame($rawPayload, $this->cacheStore()->getRaw($rowKey));
            $this->assertNotEmpty($this->pending());
            $this->assertTrue($pdo->inTransaction());

            $pdo->rollBack();
            $this->assertSame('Before', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'Before');
        } finally {
            if ($pdo->inTransaction()) {
                $pdo->rollBack();
            }
            $pdo->exec('PRAGMA defer_foreign_keys = OFF');
            $pdo->exec('PRAGMA foreign_keys = OFF');
        }
    }

    public static function finalizationEvents(): array
    {
        return [
            'application terminate' => ['terminate'],
            'job processed' => ['processed'],
            'job exception' => ['exception'],
        ];
    }

    #[DataProvider('finalizationEvents')]
    public function test_lifecycle_finalization_drains_before_read(string $kind): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $versionKey = $this->cacheKeys()->version($table);
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $before = $this->cacheStore()->getRaw($versionKey) ?? '0';

        $this->withThrowingCommit(function () use ($kind, $versionKey, $before): void {
            try {
                DB::transaction(function (): void {
                    Author::query()->whereKey(1)->update(['name' => 'Finalized']);
                });
            } catch (\RuntimeException) {
            }
            $this->assertNotEmpty($this->pending());

            if ($kind === 'terminate') {
                $this->app->terminate();
            } else {
                $job = \Mockery::mock(Job::class);
                if ($kind === 'processed') {
                    event(new JobProcessed('sync', $job));
                } else {
                    event(new JobExceptionOccurred('sync', $job, new \RuntimeException('failed')));
                }
            }

            $this->assertSame([], $this->pending());
            $this->assertSame(
                (string) ((int) $before + 1),
                $this->cacheStore()->getRaw($versionKey),
            );
            $this->assertSame('Finalized', Author::query()->findOrFail(1)->name);
            $this->assertWarmFind(1, 'Finalized');
        });
    }

    public function test_recovery_invalidation_failure_bypasses_cache_after_reconciliation(): void
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $this->withThrowingCommit(function () use ($table): void {
            try {
                DB::transaction(function (): void {
                    Author::query()->whereKey(1)->update(['name' => 'After']);
                });
            } catch (\RuntimeException) {
            }

            $this->cacheStore()->setRaw($this->cacheKeys()->version($table), 'invalid-counter', 60);
            DB::flushQueryLog();
            DB::enableQueryLog();
            try {
                $this->assertSame('After', Author::query()->findOrFail(1)->name);
                $this->assertCount(1, DB::getQueryLog());
            } finally {
                DB::disableQueryLog();
            }
            $this->assertFalse(app(CacheRuntime::class)->available());
            $this->assertSame('1', $this->cacheStore()->getRaw($this->cacheKeys()->disabled()));
        });
    }

    public function test_mysql_opaque_global_invalidation_survives_implicit_commit_and_rollback(): void
    {
        if (DB::connection()->getDriverName() !== 'mysql') {
            $this->markTestSkipped('Implicit commit requires MySQL');
        }

        $epochKey = $this->cacheKeys()->epoch();
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $before = $this->cacheStore()->getRaw($epochKey);
        DB::beginTransaction();
        Author::query()->toBase()
            ->fromRaw('authors /* opaque write target */')
            ->where('id', 1)
            ->update(['name' => 'OpaqueCommitted']);
        $this->assertSame([], $this->pending());
        Post::query()->truncate();
        DB::rollBack();

        $this->assertSame((string) ((int) $before + 1), $this->cacheStore()->getRaw($epochKey));
        $this->assertSame('OpaqueCommitted', Author::query()->findOrFail(1)->name);
        $this->assertWarmFind(1, 'OpaqueCommitted');
    }

    public function test_physical_inspection_failure_disables_cache_and_retains_pending(): void
    {
        $this->assertSame('Before', Author::query()->findOrFail(1)->name);
        $connection = DB::connection();
        $original = $connection->getPdo();
        $fixture = $this->createInspectionThrowingPdo();
        $connection->setPdo($fixture);
        try {
            DB::beginTransaction();
            Author::query()->whereKey(1)->update(['name' => 'InspectionFailure']);
            $fixture->shouldThrow = true;
            $this->assertSame('InspectionFailure', Author::query()->findOrFail(1)->name);
            $this->assertNotEmpty($this->pending());
            $this->assertFalse(app(CacheRuntime::class)->available());
            $this->assertSame('1', $this->cacheStore()->getRaw($this->cacheKeys()->disabled()));
            DB::flushQueryLog();
            DB::enableQueryLog();
            try {
                $this->assertSame('InspectionFailure', Author::query()->findOrFail(1)->name);
                $this->assertCount(1, DB::getQueryLog());
            } finally {
                DB::disableQueryLog();
            }
        } finally {
            $fixture->shouldThrow = false;
            if ($fixture->inTransaction()) {
                $fixture->rollBack();
            }
            $connection->setPdo($original);
        }
    }

    private function withThrowingCommit(callable $callback): void
    {
        $connection = DB::connection();
        $original = $connection->getPdo();
        $fixture = $this->createThrowingCommitPdo();
        $connection->setPdo($fixture);
        try {
            $callback($fixture, $original);
        } finally {
            if ($fixture->inTransaction()) {
                $fixture->rollBack();
            }
            $connection->setPdo($original);
        }
    }

    private function createThrowingCommitPdo(): \PDO
    {
        return $this->createPdoFixture(true);
    }

    private function createInspectionThrowingPdo(): \PDO
    {
        return $this->createPdoFixture(false, true);
    }

    private function createPdoFixture(bool $throwCommit, bool $throwInspect = false): \PDO
    {
        $config = DB::connection()->getConfig();
        $driver = $config['driver'] ?? 'sqlite';

        if ($driver === 'sqlite') {
            $dsn = "sqlite:{$config['database']}";
            $username = null;
            $password = null;
        } else {
            $dsn = "{$driver}:host={$config['host']};port={$config['port']};dbname={$config['database']}";
            $username = $config['username'] ?? 'root';
            $password = $config['password'] ?? '';
        }

        $original = DB::connection()->getPdo();
        $options = [
            \PDO::ATTR_ERRMODE => $original->getAttribute(\PDO::ATTR_ERRMODE),
            \PDO::ATTR_DEFAULT_FETCH_MODE => $original->getAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE),
        ];

        foreach ([\PDO::ATTR_EMULATE_PREPARES, \PDO::ATTR_STRINGIFY_FETCHES] as $attribute) {
            try {
                $options[$attribute] = $original->getAttribute($attribute);
            } catch (\Throwable) {
            }
        }

        return new class($dsn, $username, $password, $options, $throwCommit, $throwInspect) extends \PDO
        {
            public bool $shouldThrow = false;

            private bool $throwCommit;

            private bool $throwInspect;

            private bool $thrown = false;

            public function __construct($dsn, $username, $password, $options, bool $throwCommit, bool $throwInspect)
            {
                parent::__construct($dsn, $username, $password, $options);
                $this->throwCommit = $throwCommit;
                $this->throwInspect = $throwInspect;
            }

            public function commit(): bool
            {
                $result = parent::commit();
                if ($this->throwCommit && !$this->thrown) {
                    $this->thrown = true;
                    throw new \RuntimeException('Lost acknowledgement');
                }

                return $result;
            }

            public function inTransaction(): bool
            {
                if ($this->throwInspect && $this->shouldThrow) {
                    throw new \RuntimeException('Inspection unavailable');
                }

                return parent::inTransaction();
            }
        };
    }

    /** @return array{string, string|null} */
    private function warmRowPayload(int $id): array
    {
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
        $rowKey = $this->cacheKeys()->row($table, $generation, 'i:' . $id);
        $payload = $this->cacheStore()->getRaw($rowKey);
        $this->assertNotNull($payload);

        return [$rowKey, $payload];
    }

    private function assertWarmFind(int $id, string $name): void
    {
        DB::flushQueryLog();
        DB::enableQueryLog();
        try {
            $this->assertSame($name, Author::query()->findOrFail($id)->name);
            $this->assertSame([], DB::getQueryLog());
        } finally {
            DB::disableQueryLog();
        }
    }

    private function pending(): array
    {
        return array_values(app(PendingInvalidations::class)->tables['testing'] ?? []);
    }
}
