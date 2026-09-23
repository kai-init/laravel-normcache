<?php

namespace NormCache\Tests\Benchmark;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;

final class ClusterPipelineBenchmarkTest extends TestCase
{
    public function test_cluster_pipeline_benchmark(): void
    {
        DB::table('countries')->insert(['id' => 1, 'name' => 'AU', 'created_at' => now(), 'updated_at' => now()]);
        DB::table('authors')->insert(['id' => 1, 'name' => 'Author', 'country_id' => 1, 'created_at' => now(), 'updated_at' => now()]);
        DB::table('tags')->insert(['id' => 1, 'name' => 'Tag', 'created_at' => now(), 'updated_at' => now()]);
        DB::table('author_tag')->insert(['author_id' => 1, 'tag_id' => 1, 'notes' => null]);
        DB::table('posts')->insert([
            'id' => 1,
            'title' => 'Post',
            'views' => 1,
            'published' => true,
            'author_id' => 1,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $tables = (int) (getenv('BENCH_TABLES') ?: 2);
        $query = match ($tables) {
            2 => fn() => RawPost::query()->toBase()
                ->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.id')->get(),
            3 => fn() => RawPost::query()->toBase()
                ->join('authors', 'authors.id', '=', 'posts.author_id')
                ->join('countries', 'countries.id', '=', 'authors.country_id')
                ->select('posts.id')->get(),
            5 => fn() => RawPost::query()->toBase()
                ->join('authors', 'authors.id', '=', 'posts.author_id')
                ->join('countries', 'countries.id', '=', 'authors.country_id')
                ->join('author_tag', 'author_tag.author_id', '=', 'authors.id')
                ->join('tags', 'tags.id', '=', 'author_tag.tag_id')
                ->select('posts.id')->get(),
            default => throw new \InvalidArgumentException('Unsupported BENCH_TABLES'),
        };

        $query();
        $samples = [];
        $iterations = 500;

        for ($round = 0; $round < 5; $round++) {
            $start = hrtime(true);

            for ($i = 0; $i < $iterations; $i++) {
                $query();
            }

            $samples[] = (hrtime(true) - $start) / $iterations / 1000;
        }

        sort($samples, SORT_NUMERIC);
        fwrite(STDOUT, "\nBENCH " . json_encode([
            'tables' => $tables,
            'median_us' => $samples[2],
            'samples_us' => $samples,
        ], JSON_THROW_ON_ERROR) . "\n");

        $this->assertTrue(true);
    }
}
