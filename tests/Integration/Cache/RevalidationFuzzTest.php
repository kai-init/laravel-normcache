<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Event;
use NormCache\Events\QueryCacheRepaired;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;

final class RevalidationFuzzTest extends TestCase
{
    private const ROUNDS = 500;

    private const SEEDED_ROWS = 200;

    private int $authorId;

    private int $nextId;

    protected function setUp(): void
    {
        parent::setUp();

        config()->set('normcache.revalidation', true);
        $this->app->forgetInstance(CacheConfig::class);
        $this->app->forgetScopedInstances();

        $this->authorId = (int) Author::query()->create(['name' => 'Author'])->getKey();
        $this->seedRows();
    }

    private function seedRows(): void
    {
        $rows = [];

        for ($index = 1; $index <= self::SEEDED_ROWS; $index++) {
            $rows[] = $this->row($index);
        }

        RawPost::query()->toBase()->insert($rows);
        $this->nextId = self::SEEDED_ROWS + 1;
    }

    /** @return array<string, mixed> */
    private function row(int $index): array
    {
        return [
            'title' => 'Post ' . $index,
            'views' => $index % 37,
            'published' => $index % 3 !== 0,
            'author_id' => $this->authorId,
            'created_at' => now(),
            'updated_at' => now(),
        ];
    }

    private function cachedRead(): Collection
    {
        return RawPost::query()->toBase()
            ->where('published', true)
            ->orderBy('views')
            ->orderBy('id')
            ->get();
    }

    private function directRead(): Collection
    {
        return RawPost::query()->toBase()
            ->where('published', true)
            ->orderBy('views')
            ->orderBy('id')
            ->internal()
            ->get();
    }

    private function existingId(): ?int
    {
        $row = RawPost::query()->toBase()->internal()->inRandomOrder()->first();

        return $row === null ? null : (int) $row->id;
    }

    public function test_the_cached_result_matches_an_uncached_read_under_churn(): void
    {
        mt_srand(20260811);

        $repaired = 0;
        Event::listen(
            QueryCacheRepaired::class,
            static function () use (&$repaired): void {
                $repaired++;
            },
        );

        $this->cachedRead();

        for ($round = 1; $round <= self::ROUNDS; $round++) {
            $action = mt_rand(1, 5);
            $id = $this->existingId();

            match (true) {
                $action === 1 && $id !== null => $this->updateNonPredicate($id),
                $action === 2 && $id !== null => $this->updatePredicate($id, $round),
                $action === 3 => $this->insertRow(),
                $action === 4 && $id !== null => $this->deleteRow($id),
                default => null,
            };

            $this->assertEquals(
                $this->directRead(),
                $this->cachedRead(),
                "cached result diverged from an uncached read at round {$round} (action {$action})",
            );
        }

        $this->assertGreaterThan(
            0,
            $repaired,
            'revalidation never engaged, so the comparison above proved nothing',
        );
    }

    private function updateNonPredicate(int $id): void
    {
        RawPost::query()->toBase()->where('id', $id)->update([
            'title' => 'changed ' . mt_rand(),
        ]);
    }

    private function updatePredicate(int $id, int $round): void
    {
        RawPost::query()->toBase()->where('id', $id)->update(
            mt_rand(0, 1) === 0
                ? ['published' => mt_rand(0, 1) === 1]
                : ['views' => $round % 37],
        );
    }

    private function insertRow(): void
    {
        RawPost::query()->toBase()->insert($this->row($this->nextId++));
    }

    private function deleteRow(int $id): void
    {
        RawPost::query()->toBase()->where('id', $id)->delete();
    }
}
