<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Redis;
use NormCache\Cache\CacheRuntime;
use NormCache\Events\CacheInvalidated;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\RedisScripts;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use PHPUnit\Framework\Attributes\DataProvider;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

final class RedisScriptFailureTest extends TestCase
{
    #[DataProvider('scriptStates')]
    public function test_failed_invalidation_disables_reads_and_reports_failure(bool $loaded): void
    {
        $author = Author::create(['name' => 'Before']);
        Author::find($author->id);
        $table = app(TableIdentityResolver::class)->resolve(DB::connection(), 'authors');
        $this->cacheStore()->setRawForever($this->cacheKeys()->version($table), 'invalid-counter');
        $redis = Redis::connection('normcache-test');

        if ($loaded) {
            $redis->command('script', ['load', RedisScripts::get('invalidate_table')]);
        } else {
            $redis->command('script', ['flush']);
        }

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
}
