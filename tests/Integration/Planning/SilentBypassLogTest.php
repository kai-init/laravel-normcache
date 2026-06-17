<?php

namespace NormCache\Tests\Integration\Planning;

use Illuminate\Support\Facades\Log;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class SilentBypassLogTest extends TestCase
{
    public function test_logs_warning_on_unsafe_dependency_bypass()
    {
        config(['app.debug' => true]);

        Log::shouldReceive('warning')
            ->once()
            ->withArgs(function ($msg) {
                return str_contains($msg, 'unsafe dependency inference');
            });

        Author::whereHas('posts', fn($q) => $q->whereRaw('1 = 1'))->get();
    }

    public function test_logs_dependency_bypass_warning_when_events_and_debugbar_are_disabled()
    {
        config([
            'app.debug' => true,
            'normcache.events' => false,
            'normcache.debugbar' => false,
        ]);

        Log::shouldReceive('warning')
            ->once()
            ->withArgs(function ($msg) {
                return str_contains($msg, 'unsafe dependency inference');
            });

        Author::whereHas('posts', fn($q) => $q->whereRaw('1 = 1'))->get();
    }
}
