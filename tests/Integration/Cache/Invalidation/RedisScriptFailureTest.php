<?php

namespace NormCache\Tests\Integration\Cache\Invalidation;

use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\CacheRuntime;
use NormCache\Enums\MutationType;
use NormCache\Events\CacheInvalidated;
use NormCache\Facades\NormCache;
use NormCache\Invalidator;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\RedisScripts;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class RedisScriptFailureTest extends TestCase
{
    public function test_failed_global_invalidation_disables_reads_across_scopes(): void
    {
        $this->cacheStore()->setRawForever($this->cacheKeys()->epoch(), 'invalid-counter');
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->exactly(2))->method('log')->with(
            $this->anything(),
            $this->anything(),
            $this->callback(fn(array $context): bool => in_array(
                $context['event'],
                ['opaque_write_global_invalidation', 'global_invalidation_failed'],
                true,
            )),
        );
        $this->app->instance(LoggerInterface::class, $logger);
        $this->app->forgetScopedInstances();

        $runtime = app(CacheRuntime::class);
        $query = Author::query()->toBase();
        $query->from = 'authors opaque write target';
        $invalidator = app(Invalidator::class);
        $invalidator->afterWrite($query, MutationType::UPDATE, true);

        $this->assertFalse($runtime->available());
        $this->assertSame('1', $this->cacheStore()->getRaw($this->cacheKeys()->disabled()));
        $this->app->forgetScopedInstances();
        $runtime = app(CacheRuntime::class);
        $this->assertTrue($runtime->available());
        $this->assertFalse($runtime->readable());
    }

    public function test_invalidation_failure_marker_survives_scopes_and_enable_flushes_stale_rows(): void
    {
        $author = Author::create(['name' => 'Before']);
        $this->assertSame('Before', Author::find($author->id)->name);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($table);

        $versionKey = $this->cacheKeys()->version($table);
        $disabledKey = $this->cacheKeys()->disabled();
        $epochKey = $this->cacheKeys()->epoch();
        $previousEpoch = (int) ($this->cacheStore()->getRaw($epochKey) ?? '0');
        $this->cacheStore()->setRawForever($versionKey, 'invalid-counter');

        Author::where('id', $author->id)->update(['name' => 'After']);

        $this->assertSame('1', $this->cacheStore()->getRaw($disabledKey));
        $this->app->forgetScopedInstances();
        $this->assertFalse(app(CacheRuntime::class)->readable());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('After', Author::find($author->id)->name);
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());

        // The Redis counter fault has to be repaired before re-enabling.
        $this->cacheStore()->setRawForever($versionKey, '0');
        $this->assertSame($previousEpoch + 1, NormCache::enableCache());
        $this->assertNull($this->cacheStore()->getRaw($disabledKey));
        $this->app->forgetScopedInstances();

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('After', Author::find($author->id)->name);
        $this->assertSame('After', Author::find($author->id)->name);
        DB::disableQueryLog();
        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_invalidation_failure_logs_when_the_shared_disable_flag_cannot_be_written(): void
    {
        $author = Author::create(['name' => 'Before']);
        $this->assertSame('Before', Author::find($author->id)->name);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->assertNotNull($table);
        $this->cacheStore()->setRawForever($this->cacheKeys()->version($table), 'invalid-counter');

        $originalConnection = Redis::connection('normcache-test');

        if (!$originalConnection instanceof PhpRedisConnection || $originalConnection instanceof PhpRedisClusterConnection) {
            $this->markTestSkipped('Fault injection uses the standalone PhpRedis connection.');
        }

        $originalStore = $this->cacheStore();
        $originalManager = $this->app->make('redis');
        $connection = new class($originalConnection->client()) extends PhpRedisConnection
        {
            public function set($key, $value, $expireResolution = null, $expireTTL = null, $flag = null)
            {
                return false;
            }
        };
        $manager = new class($connection)
        {
            public function __construct(private PhpRedisConnection $connection) {}

            public function connection($name = null): PhpRedisConnection
            {
                return $this->connection;
            }

            public function purge(string $name): void {}
        };
        $events = [];
        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->exactly(2))->method('log')->willReturnCallback(
            function (string $level, string $message, array $context) use (&$events): void {
                $this->assertSame(LogLevel::CRITICAL, $level);
                $events[] = $context['event'];
            },
        );
        $this->app->instance('redis', $manager);
        Redis::clearResolvedInstance('redis');
        $this->app->instance(RedisStore::class, new RedisStore('normcache-test'));
        $this->app->instance(LoggerInterface::class, $logger);
        $this->app->forgetScopedInstances();

        try {
            Author::where('id', $author->id)->update(['name' => 'After']);

            $this->assertSame([
                'invalidation_disable_failed',
                'invalidation_failed',
            ], $events);
            $this->assertFalse(app(CacheRuntime::class)->available());
            $this->assertNull($originalStore->getRaw($this->cacheKeys()->disabled()));
            $this->assertSame('After', Author::find($author->id)->name);
        } finally {
            $this->app->instance('redis', $originalManager);
            Redis::clearResolvedInstance('redis');
            $this->app->instance(RedisStore::class, $originalStore);
            $this->app->forgetScopedInstances();
        }
    }

    #[DataProvider('scriptStates')]
    public function test_failed_invalidation_disables_reads_and_reports_failure(bool $loaded): void
    {
        $author = Author::create(['name' => 'Before']);
        Author::find($author->id);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->cacheStore()->setRawForever($this->cacheKeys()->version($table), 'invalid-counter');
        $redis = Redis::connection('normcache-test');

        $this->setInvalidationScriptState($redis, $loaded);

        $logger = $this->createMock(LoggerInterface::class);
        $logger->expects($this->once())->method('log')->with(
            LogLevel::CRITICAL,
            $this->anything(),
            $this->callback(fn(array $context): bool => $context['event'] === 'invalidation_failed'),
        );
        $this->app->instance(LoggerInterface::class, $logger);
        $this->app->forgetScopedInstances();
        Event::fake([CacheInvalidated::class]);

        Author::where('id', $author->id)->update(['name' => 'After']);

        $this->assertSame('After', Author::find($author->id)->name);
        $this->assertSame('After', Author::where('name', 'After')->first()->name);
        $this->assertFalse(app(CacheRuntime::class)->available());
        Event::assertNotDispatched(CacheInvalidated::class);
    }

    public static function scriptStates(): array
    {
        return ['evalsha' => [true], 'eval fallback' => [false]];
    }

    private function setInvalidationScriptState($redis, bool $loaded): void
    {
        $arguments = $loaded
            ? ['load', RedisScripts::get('invalidate_tables')]
            : ['flush'];

        if ($redis instanceof PhpRedisClusterConnection) {
            foreach ($redis->client()->_masters() as $master) {
                $redis->client()->rawCommand($master, 'SCRIPT', ...$arguments);
            }

            return;
        }

        if ($redis instanceof PredisClusterConnection) {
            foreach ($redis->client() as $node) {
                $node->executeCommand($redis->client()->createCommand('script', $arguments));
            }

            return;
        }

        $redis->command('script', $arguments);
    }
}
