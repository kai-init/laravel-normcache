<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;

/** Cold-path SCAN machinery for flushes and diagnostics. */
final class RedisScanner
{
    public function __construct(private readonly Connection $connection) {}

    public function scanPattern(string $pattern): array
    {
        $keys = match (true) {
            $this->connection instanceof PhpRedisClusterConnection => $this->scanPhpRedisClusterKeys($pattern),
            $this->connection instanceof PredisClusterConnection => $this->scanPredisClusterKeys($pattern),
            default => $this->scanKeys($pattern),
        };

        $prefix = $this->connectionPrefix();

        if ($prefix === '') {
            return $keys;
        }

        return array_map(
            static fn(string $key) => str_starts_with($key, $prefix) ? substr($key, strlen($prefix)) : $key,
            $keys,
        );
    }

    public function scanPatterns(array $patterns): array
    {
        $matched = [];

        foreach ($this->groupPatterns(array_unique(array_filter($patterns, 'is_string'))) as $group) {
            $scanPattern = count($group) === 1 ? $group[0] : $this->commonPattern($group);
            $scanPatterns = $scanPattern === '*' ? $group : [$scanPattern];

            foreach ($scanPatterns as $pattern) {
                foreach ($this->scanPattern($pattern) as $key) {
                    foreach ($group as $candidate) {
                        if (fnmatch($candidate, $key)) {
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
        $prefix = $this->connectionPrefix();

        $this->executeScan(
            function (&$cursor) use ($pattern, $prefix) {
                $pattern = $prefix . $pattern;

                return $this->isPhpRedis()
                    ? $this->connection->client()->scan($cursor, $pattern, 1000)
                    : $this->connection->scan($cursor, ['match' => $pattern, 'count' => 1000]);
            },
            static function (array $chunk) use (&$keys): void {
                array_push($keys, ...$chunk);
            },
        );

        return $keys;
    }

    private function scanPredisClusterKeys(string $pattern): array
    {
        $keys = [];
        $pattern = $this->connectionPrefix() . $pattern;

        foreach ($this->connection->client() as $node) {
            $this->executeScan(
                fn($cursor) => $node->scan($cursor, ['match' => $pattern, 'count' => 1000]),
                static function (array $chunk) use (&$keys): void {
                    array_push($keys, ...$chunk);
                },
            );
        }

        return array_values(array_unique($keys));
    }

    private function scanPhpRedisClusterKeys(string $pattern): array
    {
        $keys = [];
        $client = $this->connection->client();
        $pattern = $this->connectionPrefix() . $pattern;

        foreach ($client->_masters() as $node) {
            $this->executeScan(
                fn(&$cursor) => $client->scan($cursor, $node, $pattern, 1000),
                static function (array $chunk) use (&$keys): void {
                    array_push($keys, ...$chunk);
                },
            );
        }

        return $keys;
    }

    private function groupPatterns(array $patterns): array
    {
        $groups = [];

        foreach ($patterns as $pattern) {
            preg_match('/^\{[^{}]+\}:/', $pattern, $match);
            $groups[$match[0] ?? ''][] = $pattern;
        }

        return array_values($groups);
    }

    private function commonPattern(array $patterns): string
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

    private function isPhpRedis(): bool
    {
        return $this->connection instanceof PhpRedisConnection;
    }

    private function connectionPrefix(): string
    {
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
            $cursor = null;

            do {
                $chunk = $scanner($cursor);

                if (!empty($chunk)) {
                    $processor($chunk);
                }
            } while ($cursor);

            return;
        }

        $cursor = '0';

        do {
            $result = $scanner($cursor);

            if (!is_array($result) || !isset($result[1])) {
                return;
            }

            [$cursor, $chunk] = $result;

            if (!empty($chunk)) {
                $processor($chunk);
            }
        } while ($cursor !== '0');
    }
}
