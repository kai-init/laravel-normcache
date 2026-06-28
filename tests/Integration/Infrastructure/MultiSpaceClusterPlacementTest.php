<?php

namespace NormCache\Tests\Integration\Infrastructure;

use NormCache\Tests\Fixtures\Models\CatalogTag;
use NormCache\Tests\Fixtures\Models\ReportingCountry;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\TestCase;
use Redis;

// Against a real cluster (composer test:cluster): three models in three spaces each
// run their full lifecycle and physically land on three distinct master nodes.
class MultiSpaceClusterPlacementTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        if (!(env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true)) {
            $this->markTestSkipped('Requires a Redis Cluster (composer test:cluster).');
        }

        $this->setClusterMode(true);
    }

    public function test_three_spaces_run_and_land_on_distinct_master_nodes(): void
    {
        // 1. Each space's write + cached read succeeds only if its keys co-locate.
        SpacedPost::create(['title' => 'Article', 'author_id' => 1]);
        $this->assertSame('Article', SpacedPost::query()->get()->first()->title);

        CatalogTag::create(['name' => 'Widgets']);
        $this->assertSame('Widgets', CatalogTag::query()->get()->first()->name);

        ReportingCountry::create(['name' => 'Atlantis']);
        $this->assertSame('Atlantis', ReportingCountry::query()->get()->first()->name);

        // 2. Each space's keys physically reside on its own master node.
        $contentNode = $this->masterHolding('nc:content');
        $catalogNode = $this->masterHolding('nc:catalog');
        $reportingNode = $this->masterHolding('nc:reporting');

        $this->assertNotNull($contentNode, 'content keys must exist on a master node');
        $this->assertNotNull($catalogNode, 'catalog keys must exist on a master node');
        $this->assertNotNull($reportingNode, 'reporting keys must exist on a master node');

        // 3. Distribution: the three spaces spread across three distinct shards.
        $this->assertCount(
            3,
            array_unique([$contentNode, $catalogNode, $reportingNode]),
            'content/catalog/reporting must each land on a different master node',
        );
    }

    // Master port whose keyspace holds this space tag's keys, or null.
    private function masterHolding(string $hashTag): ?int
    {
        foreach ($this->masterPorts() as $port) {
            $node = new Redis;
            $node->connect('127.0.0.1', $port);

            // KEYS treats { } as literal; only the owning node returns this space's keys.
            if (!empty($node->keys('{' . $hashTag . '}:*'))) {
                $node->close();

                return $port;
            }

            $node->close();
        }

        return null;
    }

    /** @return list<int> master node ports, from CLUSTER SLOTS */
    private function masterPorts(): array
    {
        $probe = new Redis;
        $probe->connect('127.0.0.1', 7010);
        $slots = $probe->rawCommand('CLUSTER', 'SLOTS');
        $probe->close();

        $ports = [];
        foreach ($slots as $range) {
            $ports[] = (int) $range[2][1]; // [start, end, [masterIp, masterPort, id], ...]
        }

        return array_values(array_unique($ports));
    }
}
