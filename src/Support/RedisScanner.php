<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Predis\Cluster\RedisStrategy;

/** Cold-path SCAN machinery (flushes, diagnostics), split out of RedisStore's hot read/write path. */
final class RedisScanner
{
    public function __construct(private readonly Connection $connection) {}

    public function scanPattern(string $pattern): array
    {
        if ($this->connection instanceof PhpRedisClusterConnection) {
            $keys = $this->scanPhpRedisClusterKeys($pattern);
        } elseif ($this->connection instanceof PredisClusterConnection) {
            $keys = $this->scanPredisClusterKeys($pattern);
        } else {
            $keys = $this->scanKeys($pattern);
        }

        $connectionPrefix = $this->connectionPrefix();

        if ($connectionPrefix === '') {
            return $keys;
        }

        return array_map(
            static fn($k) => str_starts_with($k, $connectionPrefix) ? substr($k, strlen($connectionPrefix)) : $k,
            $keys
        );
    }

    public function scanPatterns(array $patterns): array
    {
        $patterns = array_values(array_unique(array_filter($patterns, 'is_string')));

        if ($patterns === []) {
            return [];
        }

        $matched = [];

        foreach ($this->groupPatternsByHashTag($patterns) as $group) {
            $scanPatterns = count($group) === 1
                ? $group
                : [$this->commonScanPattern($group)];

            if ($scanPatterns === ['*']) {
                $scanPatterns = $group;
            }

            foreach ($scanPatterns as $scanPattern) {
                foreach ($this->scanPattern($scanPattern) as $key) {
                    foreach ($group as $pattern) {
                        if (fnmatch($pattern, $key)) {
                            $matched[$key] = true;
                            break;
                        }
                    }
                }
            }
        }

        return array_keys($matched);
    }

    private function scanKeys(string $pattern): array
    {
        $keys = [];
        $connectionPrefix = $this->connectionPrefix();

        $this->executeScan(
            function (&$cursor) use ($pattern, $connectionPrefix) {
                $p = $connectionPrefix . $pattern;

                return $this->isPhpRedis()
                    ? $this->connection->client()->scan($cursor, $p, 1000)
                    : $this->connection->scan($cursor, ['match' => $p, 'count' => 1000]);
            },
            function ($chunk) use (&$keys) {
                array_push($keys, ...$chunk);
            }
        );

        return $keys;
    }

    private function scanPredisClusterKeys(string $pattern): array
    {
        $targeted = $this->scanPredisClusterSlotKeys($pattern);
        if ($targeted !== null) {
            return $targeted;
        }

        $keys = [];
        $connectionPrefix = $this->connectionPrefix();

        foreach ($this->connection->client() as $node) {
            $this->executeScan(
                fn($cursor) => $node->scan($cursor, ['match' => $connectionPrefix . $pattern, 'count' => 1000]),
                function ($chunk) use (&$keys) {
                    array_push($keys, ...$chunk);
                }
            );
        }

        return array_values(array_unique($keys));
    }

    private function scanPredisClusterSlotKeys(string $pattern): ?array
    {
        $hashTag = $this->concreteHashTag($pattern);
        if ($hashTag === null) {
            return null;
        }

        try {
            $cluster = $this->connection->client()->getConnection();
            if (!method_exists($cluster, 'getConnectionBySlot')) {
                return null;
            }

            $slot = (new RedisStrategy)->getSlotByKey('{' . $hashTag . '}');
            $node = $cluster->getConnectionBySlot($slot);
            if (!is_object($node) || !method_exists($node, 'scan')) {
                return null;
            }
        } catch (\Throwable) {
            return null;
        }

        $keys = [];
        $connectionPrefix = $this->connectionPrefix();

        $this->executeScan(
            fn($cursor) => $node->scan($cursor, ['match' => $connectionPrefix . $pattern, 'count' => 1000]),
            function ($chunk) use (&$keys) {
                array_push($keys, ...$chunk);
            }
        );

        return array_values(array_unique($keys));
    }

    private function scanPhpRedisClusterKeys(string $pattern): array
    {
        $keys = [];
        $client = $this->connection->client();
        $connectionPrefix = $this->connectionPrefix();

        foreach ($client->_masters() as $node) {
            $this->executeScan(
                function (&$cursor) use ($client, $node, $pattern, $connectionPrefix) {
                    return $client->scan($cursor, $node, $connectionPrefix . $pattern, 1000);
                },
                function ($chunk) use (&$keys) {
                    array_push($keys, ...$chunk);
                }
            );
        }

        return $keys;
    }

    /** @return list<list<string>> */
    private function groupPatternsByHashTag(array $patterns): array
    {
        $groups = [];

        foreach ($patterns as $pattern) {
            $group = preg_match('/^\{[^{}]+\}:/', $pattern, $matches) === 1
                ? $matches[0]
                : '';
            $groups[$group][] = $pattern;
        }

        return array_values($groups);
    }

    private function commonScanPattern(array $patterns): string
    {
        $prefix = array_shift($patterns);

        foreach ($patterns as $pattern) {
            $length = min(strlen($prefix), strlen($pattern));
            $i = 0;

            while ($i < $length && $prefix[$i] === $pattern[$i]) {
                $i++;
            }

            $prefix = substr($prefix, 0, $i);
        }

        return $prefix . '*';
    }

    private function concreteHashTag(string $pattern): ?string
    {
        if (!preg_match('/\{([^{}]+)\}/', $pattern, $matches)) {
            return null;
        }

        $tag = $matches[1];

        return str_contains($tag, '*') || str_contains($tag, '?')
            ? null
            : $tag;
    }

    private function isPhpRedis(): bool
    {
        // PhpRedisClusterConnection extends PhpRedisConnection, so this covers both.
        return $this->connection instanceof PhpRedisConnection;
    }

    private function connectionPrefix(): string
    {
        // *ClusterConnection extends *Connection, so these cover both cluster and standalone.
        if ($this->connection instanceof PhpRedisConnection) {
            return (string) $this->connection->client()->getOption(\Redis::OPT_PREFIX);
        }

        if ($this->connection instanceof PredisConnection) {
            $prefix = $this->connection->client()->getOptions()->prefix ?? null;
            if (is_object($prefix) && method_exists($prefix, 'getPrefix')) {
                return (string) $prefix->getPrefix();
            }
        }

        return '';
    }

    /**
     * @param  \Closure(mixed &): mixed  $scanner
     * @param  \Closure(array<mixed>): void  $processor
     */
    private function executeScan(\Closure $scanner, \Closure $processor): void
    {
        if ($this->isPhpRedis()) {
            // phpredis 6.x SCAN/SSCAN require null to start; updates cursor by reference.
            $cursor = null;
            while (true) {
                $chunk = $scanner($cursor);
                if (!empty($chunk)) {
                    $processor($chunk);
                }
                if (!$cursor) {
                    break;
                }
            }

            return;
        }

        // Predis returns [$cursor, $keys]; '0' signals completion.
        $cursor = '0';
        do {
            $result = $scanner($cursor);
            if (!is_array($result) || !isset($result[1])) {
                break;
            }
            [$cursor, $chunk] = $result;
            if (!empty($chunk)) {
                $processor($chunk);
            }
        } while ($cursor !== '0');
    }
}
