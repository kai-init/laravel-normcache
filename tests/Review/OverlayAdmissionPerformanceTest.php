<?php

namespace NormCache\Tests\Review;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Payload\RawResultCodec;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Values\CacheConfig;

final class OverlayAdmissionPerformanceTest extends ReviewBenchmarkCase
{
    private const ROWS = 40;

    public function test_uniform_wide_overlay_repeats_the_estimate_cost(): void
    {
        $this->measureRejectedOverlay(
            'uniform wide rows: first-two-row estimate rejects',
            static fn(int $index): int => 4000,
        );
    }

    public function test_skewed_wide_overlay_reencodes_the_full_payload(): void
    {
        $this->measureRejectedOverlay(
            'skewed rows: narrow first two rows let the estimate pass',
            static fn(int $index): int => $index <= 2 ? 32 : 4300,
        );
    }

    /** @param callable(int): int $bytesForRow */
    private function measureRejectedOverlay(string $shape, callable $bytesForRow): void
    {
        $this->seedWideRows($bytesForRow);

        $iterations = max(1, (int) (getenv('NORMCACHE_OVERSIZED_ITERATIONS') ?: 500));
        $repetitions = max(1, (int) (getenv('NORMCACHE_OVERSIZED_REPETITIONS') ?: 5));
        $query = static fn() => Post::query()
            ->orderBy('id')
            ->limit(self::ROWS)
            ->get();
        $results = [];

        foreach (['byte guard' => 50, 'row guard' => 38] as $label => $budget) {
            $results[$label] = $this->withRowBudget(
                $budget,
                function () use ($query, $iterations, $repetitions): float {
                    Redis::connection('normcache-test')->flushdb();
                    $this->app->forgetScopedInstances();
                    $query();

                    for ($index = 0; $index < 200; $index++) {
                        $query();
                    }

                    $best = INF;

                    for ($repetition = 0; $repetition < $repetitions; $repetition++) {
                        $best = min($best, $this->time($iterations, $query) / $iterations);
                    }

                    return $best;
                },
            );
        }

        $this->assertOverlayIsSkipped($query);

        $rows = DB::table('posts')
            ->withoutCache()
            ->orderBy('id')
            ->limit(self::ROWS)
            ->get()
            ->all();
        $encodedBytes = strlen($this->app->make(RawResultCodec::class)->encode($rows, '0'));
        $line = str_repeat('=', 84);

        fwrite(STDOUT, "\n{$line}\n");
        fwrite(STDOUT, "{$shape}\n");
        fwrite(STDOUT, sprintf(
            "%d rows / %.1f KiB encoded - best of %d x %d iterations\n",
            self::ROWS,
            $encodedBytes / 1024,
            $repetitions,
            $iterations,
        ));
        fwrite(STDOUT, "{$line}\n");

        foreach ($results as $label => $microseconds) {
            fwrite(STDOUT, sprintf("%-32s %12.2f us/op\n", $label, $microseconds));
        }

        fwrite(STDOUT, sprintf(
            "%-32s %12.2f us/op\n",
            'repeated admission work',
            $results['byte guard'] - $results['row guard'],
        ));
        fwrite(STDOUT, "{$line}\n\n");
    }

    private function withRowBudget(int $budget, callable $measure): float
    {
        $original = $this->app->make(CacheConfig::class);
        $config = (array) config('normcache');
        $config['auto_overlay_max_rows'] = $budget;
        $this->app->instance(CacheConfig::class, CacheConfig::fromArray($config));
        $this->app->forgetScopedInstances();

        try {
            return $measure();
        } finally {
            $this->app->instance(CacheConfig::class, $original);
            $this->app->forgetScopedInstances();
        }
    }

    private function assertOverlayIsSkipped(callable $query): void
    {
        Redis::connection('normcache-test')->flushdb();
        $this->app->forgetScopedInstances();
        $query();
        $query();

        $this->assertSame([], $this->cacheKeysMatching(':e:v'));
        $this->assertNotSame([], $this->cacheKeysMatching(':m:v'));
    }

    /** @param callable(int): int $bytesForRow */
    private function seedWideRows(callable $bytesForRow): void
    {
        $author = Author::query()->create(['name' => 'Author']);

        foreach (range(1, self::ROWS) as $index) {
            $bytes = max(2, $bytesForRow($index));

            DB::table('posts')->insert([
                'title' => "Post {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $author->getKey(),
                'metadata' => json_encode([
                    'blob' => bin2hex(random_bytes(intdiv($bytes, 2))),
                ]),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }
}
