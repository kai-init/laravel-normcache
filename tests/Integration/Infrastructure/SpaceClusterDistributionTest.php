<?php

namespace NormCache\Tests\Integration\Infrastructure;

use NormCache\Tests\Fixtures\Models\SpacedAuthor;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\TestCase;

// Against a real cluster (composer test:cluster): a spaced operation co-locates in
// one slot, and distinct spaces map to distinct slots.
class SpaceClusterDistributionTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        if (!(env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true)) {
            $this->markTestSkipped('Requires a Redis Cluster (composer test:cluster).');
        }

        $this->setClusterMode(true);
    }

    public function test_distinct_spaces_map_to_distinct_cluster_slots(): void
    {
        $default = $this->slotFor('nc');
        $content = $this->slotFor('nc:content');
        $catalog = $this->slotFor('nc:catalog');

        $this->assertNotSame($default, $content, 'content must not share the default slot');
        $this->assertNotSame($content, $catalog, 'content and catalog must be on different slots');
        $this->assertNotSame($default, $catalog, 'catalog must not share the default slot');
    }

    public function test_spaced_model_full_cycle_works_on_cluster(): void
    {
        // Passes only if the operation's keys co-locate; a cross-slot key throws CROSSSLOT.
        $author = SpacedAuthor::create(['name' => 'Ann']);
        SpacedPost::create(['title' => 'First', 'author_id' => $author->id]);

        $post = SpacedPost::query()->with('spacedAuthor')->get()->first();
        $this->assertSame('First', $post->title);
        $this->assertSame('Ann', $post->spacedAuthor->name);

        $post->update(['title' => 'Second']);
        $this->assertSame('Second', SpacedPost::query()->get()->first()->title);
    }

    // Redis Cluster hash slot: CRC16 (XMODEM) of the hash tag, mod 16384.
    private function slotFor(string $hashTag): int
    {
        $crc = 0;

        foreach (str_split($hashTag) as $char) {
            $crc ^= ord($char) << 8;

            for ($i = 0; $i < 8; $i++) {
                $crc = ($crc & 0x8000) ? (($crc << 1) ^ 0x1021) & 0xFFFF : ($crc << 1) & 0xFFFF;
            }
        }

        return $crc % 16384;
    }
}
