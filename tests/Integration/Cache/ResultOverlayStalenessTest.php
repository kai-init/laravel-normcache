<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Facades\NormCache;
use NormCache\Payload\MembershipCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use NormCache\Values\TableIdentity;

final class ResultOverlayStalenessTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);

        for ($i = 0; $i < 3; $i++) {
            DB::table('posts')->insert([
                'title' => 'Post ' . $i,
                'views' => $i,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => '2026-07-30 00:00:00',
                'updated_at' => '2026-07-30 00:00:00',
            ]);
        }
    }

    public function test_a_single_cold_execution_publishes_the_overlay_alongside_the_membership(): void
    {
        $this->overlayQuery()();

        $this->assertCount(1, $this->cacheQueryKeysWithField('m'));
        $this->assertCount(1, $this->cacheQueryKeysWithField('r'));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $rows = $this->overlayQuery()();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertSame([], $queries);
        $this->assertCount(3, $rows);
    }

    public function test_rejected_overlay_admission_is_recorded_in_the_membership(): void
    {
        DB::table('posts')->delete();
        $authorId = (int) DB::table('authors')->value('id');
        $rows = [];

        for ($index = 0; $index < 40; $index++) {
            $rows[] = [
                'title' => 'Post ' . $index,
                'views' => $index,
                'published' => true,
                // igbinary interns identical strings: 38 repeats of one 4 KiB blob
                // encode to 4 KiB, not 152 KiB, and the overlay would be admitted.
                'metadata' => json_encode([
                    'blob' => $index < 2 ? 'small' : str_pad((string) $index, 4000, 'x'),
                ], JSON_THROW_ON_ERROR),
                'author_id' => $authorId,
                'created_at' => '2026-07-30 00:00:00',
                'updated_at' => '2026-07-30 00:00:00',
            ];
        }

        DB::table('posts')->insert($rows);
        $query = static fn() => DB::table('posts')->orderBy('id')->get();
        $this->assertCount(40, $query());
        $membershipKey = $this->cacheQueryKeysWithField('m')[0] ?? null;
        $postsPrefix = $this->cacheKeys()->tablePrefix($this->postsIdentity());
        $postResults = fn(): array => array_values(array_filter(
            $this->cacheQueryKeysWithField('r'),
            static fn(string $key): bool => str_starts_with($key, $postsPrefix),
        ));

        $this->assertIsString($membershipKey);
        $raw = $this->cacheStore()->readHashField($membershipKey, 'm');
        $this->assertIsString($raw);
        $this->assertTrue(app(MembershipCodec::class)->decode($raw)->overlayRejected);
        $this->assertSame([], $postResults());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertCount(40, $query());
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
        $this->assertSame([], $postResults());
    }

    public function test_an_overlay_is_not_served_after_its_root_table_is_invalidated(): void
    {
        $this->warmOverlay();
        $this->assertNotSame([], $this->cacheQueryKeysWithField('r'));

        NormCache::invalidate(['posts']);

        $this->assertServedFromDatabase();
    }

    public function test_an_overlay_is_not_served_after_a_dependency_is_invalidated(): void
    {
        $query = fn() => DB::table('posts')
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->dependsOn(['posts', 'authors'])
            ->select('posts.*')
            ->orderBy('posts.id')
            ->get();

        $query();
        $query();

        NormCache::invalidate(['authors']);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $query();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotSame([], $queries);
    }

    public function test_an_overlay_is_not_served_after_its_tag_is_flushed(): void
    {
        $query = fn() => DB::table('posts')->tag('homepage')->orderBy('id')->get();

        $query();
        $query();

        NormCache::flushTag('homepage');

        DB::flushQueryLog();
        DB::enableQueryLog();
        $query();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotSame([], $queries);
    }

    public function test_an_overlay_is_not_served_after_a_global_epoch_flush(): void
    {
        $this->warmOverlay();

        NormCache::flushAll();

        $this->assertServedFromDatabase();
    }

    public function test_an_overlay_written_against_a_superseded_version_is_never_served(): void
    {
        $this->warmOverlay();
        $overlayKey = $this->cacheQueryKeysWithField('r')[0] ?? null;
        $this->assertIsString($overlayKey);

        $this->cacheStore()->increment($this->cacheKeys()->version($this->postsIdentity()));

        $this->assertServedFromDatabase();

        // The orphaned payload is still physically present — invalidation advances
        // counters rather than deleting keys — so unreachability, not absence, is the
        // guarantee being pinned here.
        $this->assertContains($overlayKey, $this->cacheQueryKeysWithField('r'));
    }

    private function warmOverlay(): void
    {
        $this->overlayQuery()();
        $this->overlayQuery()();
    }

    private function overlayQuery(): callable
    {
        return static fn() => DB::table('posts')->orderBy('id')->get();
    }

    private function assertServedFromDatabase(): void
    {
        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->overlayQuery()();
        $queries = DB::getQueryLog();
        DB::disableQueryLog();

        $this->assertNotSame([], $queries);
    }

    private function postsIdentity(): TableIdentity
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);

        return $identity;
    }
}
