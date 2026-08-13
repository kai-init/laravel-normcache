<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Payload\MembershipCodec;
use NormCache\Payload\RawResultCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\TableIdentity;

final class StaleOverlayTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);

        for ($i = 0; $i < 3; $i++) {
            RawPost::query()->toBase()->insert([
                'title' => 'Post ' . $i,
                'views' => $i,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => '2026-07-30 00:00:00',
                'updated_at' => '2026-07-30 00:00:00',
            ]);
        }
    }

    public function test_a_stale_overlay_is_not_served_after_a_root_version_bump(): void
    {
        $query = static fn() => RawPost::query()->toBase()->orderBy('id')->get();

        $query();
        $this->assertCount(1, $this->cacheQueryKeysWithField('m'));
        $this->assertCount(1, $this->cacheQueryKeysWithField('r'));

        $this->cacheStore()->increment($this->cacheKeys()->version($this->postsIdentity()));

        $this->assertColdCacheMiss($query);
    }

    public function test_a_membership_forged_with_a_foreign_root_version_is_not_served(): void
    {
        $query = static fn() => RawPost::query()->toBase()->orderBy('id')->get();
        $query();

        $key = $this->cacheQueryKeysWithField('m')[0] ?? null;
        $this->assertIsString($key);

        $codec = $this->app->make(MembershipCodec::class);
        $membership = $codec->decode((string) $this->cacheStore()->readHashField($key, 'm'));
        $this->assertTrue($membership->valid);

        $this->cacheStore()->writeHashField($key, 'm', $codec->encode(
            epoch: $membership->epoch,
            generation: $membership->generation,
            ids: $membership->ids,
            versions: $membership->versions,
            tagVersion: $membership->tagVersion,
            overlayRejected: $membership->overlayRejected,
            rootVersion: '999',
        ));
        $this->deleteResultOverlays();

        $this->assertColdCacheMiss($query);
    }

    public function test_a_result_payload_forged_with_a_foreign_root_version_is_not_served(): void
    {
        $query = static fn() => RawPost::query()->toBase()->where('published', true)->count();
        $query();

        $key = $this->cacheQueryKeysWithField('r')[0] ?? null;
        $this->assertIsString($key);

        $codec = $this->app->make(RawResultCodec::class);
        $result = $codec->decode((string) $this->cacheStore()->readHashField($key, 'r'));
        $this->assertTrue($result->valid);

        $this->cacheStore()->writeHashField($key, 'r', $codec->encode(
            rows: $result->rows,
            epoch: $result->epoch,
            versions: $result->versions,
            tagVersion: $result->tagVersion,
            rootVersion: '999',
        ));

        $this->assertColdCacheMiss($query);
    }

    private function postsIdentity(): TableIdentity
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($identity);

        return $identity;
    }
}
