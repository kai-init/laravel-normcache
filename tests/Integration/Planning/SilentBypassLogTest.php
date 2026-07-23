<?php

namespace NormCache\Tests\Integration\Planning;

use Illuminate\Support\Facades\Log;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\TestCase;

class SilentBypassLogTest extends TestCase
{
    public function test_logs_warning_on_unsafe_dependency_bypass(): void
    {
        config(['app.debug' => true]);

        Log::shouldReceive('warning')
            ->once()
            ->withArgs(function ($msg) {
                return str_contains($msg, 'unsafe dependency inference');
            });

        Author::whereHas('posts', fn($q) => $q->whereRaw('1 = 1'))->get();
    }

    public function test_cross_space_bypass_logs_only_the_space_warning(): void
    {
        config(['app.debug' => true]);

        Log::shouldReceive('warning')
            ->once()
            ->withArgs(function ($msg) {
                return str_contains($msg, 'not in that space')
                    && !str_contains($msg, 'unsafe dependency inference');
            });

        SpacedPost::query()->dependsOn([Author::class])->get();
    }
}
