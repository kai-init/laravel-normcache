<?php

namespace NormCache\Tests\Integration\Infrastructure\Concerns;

use Illuminate\Support\Facades\Redis as RedisFacade;
use Predis\Client as PredisClient;
use Redis;
use RedisCluster;

trait InteractsWithClusterRedis
{
    protected function requiresRedisCluster(): void
    {
        if (!(env('REDIS_CLUSTER') === 'true' || env('REDIS_CLUSTER') === true)) {
            $this->markTestSkipped('Requires a Redis Cluster (composer test:cluster).');
        }

        $client = RedisFacade::connection('normcache-test')->client();

        if ($client instanceof RedisCluster || $client instanceof PredisClient) {
            return;
        }

        if (class_exists(Redis::class)) {
            [$host, $port] = $this->clusterProbeNode();
            $probe = new Redis;

            try {
                $probe->connect($host, $port);
                $slots = $probe->rawCommand('CLUSTER', 'SLOTS');
            } finally {
                $probe->close();
            }

            if (is_array($slots) && $slots !== []) {
                return;
            }
        }

        $this->markTestSkipped('Redis connection is not cluster-backed.');
    }

    /** @return list<array{0: string, 1: int}> */
    protected function clusterMasterNodes(): array
    {
        if (!class_exists(Redis::class)) {
            $this->markTestSkipped('Physical cluster inspection requires the phpredis extension.');
        }

        [$host, $port] = $this->clusterProbeNode();
        $probe = new Redis;

        try {
            $probe->connect($host, $port);
            $slots = $probe->rawCommand('CLUSTER', 'SLOTS');
        } finally {
            $probe->close();
        }

        $nodes = [];
        foreach ((array) $slots as $range) {
            if (!is_array($range) || !isset($range[2]) || !is_array($range[2])) {
                continue;
            }

            $masterHost = is_string($range[2][0] ?? null) ? $range[2][0] : $host;
            $masterPort = (int) ($range[2][1] ?? 0);

            if ($masterPort > 0) {
                $nodes["{$masterHost}:{$masterPort}"] = [$masterHost, $masterPort];
            }
        }

        return array_values($nodes);
    }

    protected function redisClusterSlot(string $hashTag): int
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

    /** @return list<string> */
    protected function keysForHashTag(string $hashTag, string $suffix = '*'): array
    {
        return array_values($this->cacheManager()->store()->scanPattern('{' . $hashTag . '}:' . $suffix));
    }

    protected function assertAnyKeysForHashTag(string $hashTag, string $suffix = '*'): void
    {
        $this->assertNotEmpty(
            $this->keysForHashTag($hashTag, $suffix),
            "Expected keys for {{$hashTag}}:{$suffix}.",
        );
    }

    protected function assertNoKeysForHashTag(string $hashTag, string $suffix = '*'): void
    {
        $this->assertEmpty(
            $this->keysForHashTag($hashTag, $suffix),
            "Expected no keys for {{$hashTag}}:{$suffix}.",
        );
    }

    /** @param list<string> $keys */
    protected function assertAllKeysShareHashTag(array $keys, string $hashTag): void
    {
        $this->assertNotEmpty($keys, 'Expected at least one key to inspect.');

        foreach ($keys as $key) {
            $this->assertStringStartsWith('{' . $hashTag . '}:', $key);
        }
    }

    protected function assertNoCrossSlot(callable $callback): mixed
    {
        try {
            return $callback();
        } catch (\Throwable $e) {
            $this->assertStringNotContainsString('CROSSSLOT', $e->getMessage());
            throw $e;
        }
    }

    /** @return array{0: string, 1: int} */
    protected function clusterProbeNode(): array
    {
        $node = config('database.redis.clusters.normcache-test.0');

        return [$node['host'], (int) $node['port']];
    }
}
