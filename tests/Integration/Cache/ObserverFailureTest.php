<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class ObserverFailureTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Author::query()->create(['name' => 'Author']);
    }

    public function test_a_throwing_miss_listener_does_not_abort_the_query(): void
    {
        Event::listen(QueryCacheMiss::class, static function (): void {
            throw new \RuntimeException('diagnostics exploded');
        });

        $authors = Author::query()->toBase()->orderBy('id')->get();

        $this->assertCount(1, $authors, 'a broken listener must not cost the caller its rows');
    }

    public function test_a_throwing_miss_listener_does_not_strand_the_build_lease(): void
    {
        Event::listen(QueryCacheMiss::class, static function (): void {
            throw new \RuntimeException('diagnostics exploded');
        });

        try {
            Author::query()->toBase()->orderBy('id')->get();
        } catch (\Throwable) {
        }

        $this->assertSame(
            [],
            $this->cacheKeysMatching(':build:'),
            'an owned build lease must not outlive the request that claimed it',
        );
    }

    public function test_a_throwing_hit_listener_does_not_abort_the_query(): void
    {
        Author::query()->toBase()->orderBy('id')->get();

        Event::listen(QueryCacheHit::class, static function (): void {
            throw new \RuntimeException('diagnostics exploded');
        });

        $authors = Author::query()->toBase()->orderBy('id')->get();

        $this->assertCount(1, $authors);
    }

    public function test_a_throwing_bypass_listener_does_not_abort_the_query(): void
    {
        Event::listen(QueryBypassed::class, static function (): void {
            throw new \RuntimeException('diagnostics exploded');
        });

        $authors = Author::query()->toBase()->whereRaw('1 = 1 /* opaque */')->get();

        $this->assertCount(1, $authors);
    }
}
