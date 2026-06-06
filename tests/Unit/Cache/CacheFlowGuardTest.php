<?php

namespace NormCache\Tests\Unit\Cache;

use NormCache\Cache\CacheFlowGuard;
use PHPUnit\Framework\TestCase;

class CacheFlowGuardTest extends TestCase
{
    public function test_rescue_returns_operation_result_on_success(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: false);
        $this->assertSame(42, $guard->rescue(fn() => 42, fn() => 0));
    }

    public function test_rescue_calls_fallback_when_operation_throws_and_fallback_enabled(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: true, reporter: fn() => null);
        $result = $guard->rescue(
            fn() => throw new \RuntimeException('redis down'),
            fn() => 'fallback'
        );
        $this->assertSame('fallback', $result);
    }

    public function test_rescue_rethrows_when_fallback_disabled(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: false);
        $this->expectException(\RuntimeException::class);
        $guard->rescue(fn() => throw new \RuntimeException('boom'), fn() => null);
    }

    public function test_attempt_returns_true_on_success(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: false);
        $this->assertTrue($guard->attempt(fn() => null));
    }

    public function test_attempt_returns_false_when_fallback_enabled_and_throws(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: true, reporter: fn() => null);
        $this->assertFalse($guard->attempt(fn() => throw new \RuntimeException));
    }

    public function test_attempt_rethrows_when_fallback_disabled(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: false);
        $this->expectException(\RuntimeException::class);
        $guard->attempt(fn() => throw new \RuntimeException);
    }

    public function test_fallback_disables_guard_when_fallback_enabled(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: true, reporter: fn() => null);
        $guard->fallback(new \RuntimeException);
        $this->assertFalse($guard->isEnabled());
    }

    public function test_is_enabled_true_by_default(): void
    {
        $guard = new CacheFlowGuard(fallbackEnabled: false);
        $this->assertTrue($guard->isEnabled());
    }
}
