<?php

namespace NormCache\Tests\Integration\Cache;

use NormCache\Cache\QueryEntryRepository;
use NormCache\Enums\ReadOutcome;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisProtocol;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheRead;
use NormCache\Values\CacheState;
use NormCache\Values\MembershipPayload;
use NormCache\Values\QueryPlan;

final class StaleMembershipTest extends TestCase
{
    public function test_a_version_bump_leaves_a_decodable_stale_membership(): void
    {
        $this->seedPosts(3);
        RawPost::query()->toBase()->orderBy('id')->get();
        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);

        $read = $this->readCanonicalDirectly();

        $this->assertSame(ReadOutcome::MISS, $read->outcome);
        $this->assertNotNull($read->staleMembership);
        $this->assertCount(3, $read->staleMembership->ids);
        $this->assertNotNull($read->staleMembershipRaw);
    }

    public function test_a_corrupt_entry_yields_no_stale_membership(): void
    {
        $this->seedPosts(3);
        RawPost::query()->toBase()->orderBy('id')->get();
        $this->corruptMembershipPayload();

        $read = $this->readCanonicalDirectly();

        $this->assertSame(ReadOutcome::MISS, $read->outcome);
        $this->assertNull($read->staleMembership);
        $this->assertNull($read->staleMembershipRaw);
    }

    public function test_every_cache_read_wither_preserves_the_stale_membership_fields(): void
    {
        $state = new CacheState(
            key: 'test:{nc:x:stale-membership-wither}:q:u',
            epoch: '0',
            version: '0',
            generation: '0',
            versions: [],
            tag: null,
            tagKey: null,
        );
        $membership = new MembershipPayload(valid: true, ids: ['i:1'], rootVersion: '0');
        $read = new CacheRead(
            $state,
            ReadOutcome::MISS,
            staleMembership: $membership,
            staleMembershipRaw: 'raw-payload',
        );

        $viaWithRows = $read->withRows(['row']);
        $viaWithReason = $read->withReason('some_reason');
        $viaAsRepaired = $read->asRepaired('repaired_reason');

        foreach ([$viaWithRows, $viaWithReason, $viaAsRepaired] as $wither) {
            $this->assertSame($membership, $wither->staleMembership);
            $this->assertSame('raw-payload', $wither->staleMembershipRaw);
        }
    }

    private function seedPosts(int $count): void
    {
        $author = Author::query()->create(['name' => 'Author']);

        for ($index = 1; $index <= $count; $index++) {
            RawPost::query()->toBase()->insertGetId([
                'title' => "Post {$index}",
                'views' => $index,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    private function corruptMembershipPayload(): void
    {
        $key = $this->cacheQueryKeysWithField('m')[0] ?? null;
        $this->assertIsString($key);
        $this->cacheStore()->writeHashField($key, 'm', 'corrupt');
    }

    private function readCanonicalDirectly(): CacheRead
    {
        $query = RawPost::query()->toBase();
        $root = $this->app->make(TableIdentityResolver::class)
            ->resolve($query->getConnection(), $query->from);
        $this->assertNotNull($root);

        $plan = QueryPlan::canonical($root, [$root], $query->primaryKey());
        $namespace = $this->app->make(QueryIdentity::class)->namespace(null, null);

        $membershipKey = $this->cacheQueryKeysWithField('m')[0] ?? null;
        $this->assertIsString($membershipKey);
        $raw = $this->cacheStore()->readHashField($membershipKey, 'm');
        $this->assertIsString($raw);

        $version = $this->cacheStore()->getRaw($this->cacheKeys()->version($root)) ?? '0';
        $generation = $this->cacheStore()->getRaw($this->cacheKeys()->generation($root)) ?? '0';
        $head = [RedisProtocol::HIT, $version, $generation, $raw];

        return $this->app->make(QueryEntryRepository::class)->readCanonical(
            $plan,
            $namespace,
            'stale-membership-test',
            $head,
            false,
            fn(CacheState $state, array $tokens) => null,
        );
    }
}
