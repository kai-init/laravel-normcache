<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Predis\NotSupportedException;
use Throwable;

final class RedisStore
{
    private ?Connection $connection = null;

    /** @var array<string, string> */
    private static array $shas = [];

    public function __construct(
        private string $redisConnection,
        private int $wakeTokenCount = 64,
    ) {}

    public function getRaw(string $key): ?string
    {
        $value = $this->connection()->get($key);

        return $value !== null && $value !== false ? $value : null;
    }

    public function setRaw(string $key, string $value, int $ttl): void
    {
        $this->connection()->setex($key, $ttl, $value);
    }

    public function setNxEx(string $key, string $value, int $ttl): bool
    {
        $connection = $this->connection();

        if ($connection instanceof PhpRedisConnection) {
            return $connection->client()->set($key, $value, ['nx', 'ex' => $ttl]) !== false;
        }

        $result = $connection->command('set', [$key, $value, 'EX', $ttl, 'NX']);

        return $result !== null && $result !== false;
    }

    public function delete(string|array $keys): void
    {
        $keys = array_values(array_filter(
            (array) $keys,
            static fn(mixed $key): bool => is_string($key) && $key !== '',
        ));

        if ($keys === []) {
            return;
        }

        $this->del($keys);
    }

    public function releaseBuilding(
        string $buildingKey,
        string $wakeKey,
        ?string $token = null,
        int $wakeTtl = 10,
    ): bool {
        $keys = $wakeKey !== '' ? [$buildingKey, $wakeKey] : [$buildingKey];

        return (bool) $this->script(
            RedisScripts::get('release_building'),
            $keys,
            [
                $token ?? '',
                (string) $this->wakeTokenCount,
                (string) $wakeTtl,
            ],
        );
    }

    /**
     * @param  array<string, string>  $entries
     * @param  list<string>  $versionKeys
     * @param  list<string>  $expectedVersions
     */
    public function storeVersionedPayload(
        array $entries,
        int $ttl,
        array $versionKeys,
        array $expectedVersions,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
        int $wakeTtl = 10,
    ): bool {
        $keys = [...$versionKeys, ...array_keys($entries)];

        if ($buildingKey !== null) {
            $keys[] = $buildingKey;

            if ($wakeKey !== null && $wakeKey !== '') {
                $keys[] = $wakeKey;
            }
        }

        return (bool) $this->script(
            RedisScripts::get('store_versioned_payload'),
            $keys,
            [
                (string) count($versionKeys),
                (string) count($entries),
                (string) $ttl,
                ...$expectedVersions,
                ...array_values($entries),
                $token ?? '',
                (string) $this->wakeTokenCount,
                (string) $wakeTtl,
            ],
        );
    }

    /** @return array<int, mixed> */
    public function fetchCanonical(
        string $versionKey,
        string $generationKey,
        string $tablePrefix,
        string $namespace,
        string $queryHash,
        int $maxMembershipBytes,
        int $maxMembershipRows,
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_canonical'),
            [$versionKey, $generationKey, $tablePrefix],
            [
                $namespace,
                $queryHash,
                (string) $maxMembershipBytes,
                (string) $maxMembershipRows,
            ],
        );
    }

    /** @return array<int, mixed> */
    public function fetchRow(
        string $generationKey,
        string $tablePrefix,
        string $primaryKeyToken,
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_row'),
            [$generationKey, $tablePrefix],
            [$primaryKeyToken],
        );
    }

    /** @return array<int, mixed> */
    public function fetchExact(
        string $versionKey,
        string $tablePrefix,
        string $namespace,
        string $queryHash,
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_exact'),
            [$versionKey, $tablePrefix],
            [$namespace, $queryHash],
        );
    }

    /** @param array<string, string> $rows */
    public function publishCanonical(
        string $versionKey,
        string $generationKey,
        string $membershipKey,
        array $rows,
        string $expectedVersion,
        string $expectedGeneration,
        string $membershipPayload,
        int $membershipTtl,
        int $rowTtl,
        string $buildingKey,
        string $wakeKey,
        string $token,
        int $wakeTtl,
    ): bool {
        return (bool) $this->script(
            RedisScripts::get('publish_canonical'),
            [
                $versionKey,
                $generationKey,
                $membershipKey,
                ...array_keys($rows),
                $buildingKey,
                $wakeKey,
            ],
            [
                (string) count($rows),
                $expectedVersion,
                $expectedGeneration,
                (string) $membershipTtl,
                (string) $rowTtl,
                $membershipPayload,
                ...array_values($rows),
                $token,
                (string) $this->wakeTokenCount,
                (string) $wakeTtl,
            ],
        );
    }

    /**
     * @param  array<string, string>  $guards
     * @param  array<string, string>  $rows
     */
    public function publishRepair(
        string $versionKey,
        string $generationKey,
        array $guards,
        array $rows,
        string $expectedVersion,
        string $expectedGeneration,
        int $rowTtl,
        string $buildingKey,
        string $wakeKey,
        string $token,
        int $wakeTtl,
    ): bool {
        return (bool) $this->script(
            RedisScripts::get('publish_repair'),
            [
                $versionKey,
                $generationKey,
                ...array_keys($guards),
                ...array_keys($rows),
                $buildingKey,
                $wakeKey,
            ],
            [
                (string) count($rows),
                $expectedVersion,
                $expectedGeneration,
                (string) $rowTtl,
                ...array_values($guards),
                ...array_values($rows),
                $token,
                (string) $this->wakeTokenCount,
                (string) $wakeTtl,
            ],
        );
    }

    public function increment(string $key): int
    {
        return (int) $this->connection()->incr($key);
    }

    /**
     * @param  array<string, string>  $guards  guard key => PK token
     */
    public function invalidateTableState(
        string $versionKey,
        string $generationKey,
        string $mode,
        array $guards,
        int $guardTtl,
        string $rowPrefix,
    ): void {
        $this->script(
            RedisScripts::get('invalidate_table'),
            [$versionKey, $generationKey, ...array_keys($guards)],
            [
                $mode,
                (string) $guardTtl,
                $rowPrefix,
                ...array_values($guards),
            ],
        );
    }

    public function brpop(string $key, float $timeoutSeconds): bool
    {
        $result = $this->connection()->brpop($key, $timeoutSeconds);

        return $result !== null && $result !== false;
    }

    /**
     * All keys passed to one script must share a Redis Cluster hash slot.
     *
     * @param  list<string>  $keys
     * @param  list<mixed>  $args
     */
    public function script(string $script, array $keys, array $args = []): mixed
    {
        $connection = $this->connection();
        $keyCount = count($keys);
        $arguments = [...$keys, ...$args];
        $sha = self::$shas[$script] ??= sha1($script);

        try {
            if ($connection instanceof PhpRedisConnection) {
                $result = $connection->client()->evalSha($sha, $arguments, $keyCount);
            } else {
                $result = $connection->command('evalsha', [$sha, $keyCount, ...$arguments]);
            }
        } catch (Throwable $exception) {
            if (
                !str_contains(strtolower($exception->getMessage()), 'noscript')
                && !($exception instanceof NotSupportedException
                    && str_contains($exception->getMessage(), 'EVALSHA'))
            ) {
                throw $exception;
            }

            if ($connection instanceof PhpRedisConnection) {
                return $connection->eval(
                    $script,
                    $keyCount,
                    ...$arguments,
                );
            }

            return $connection->command(
                'eval',
                [$script, $keyCount, ...$arguments],
            );
        }

        if ($result === false && $connection instanceof PhpRedisConnection) {
            $client = $connection->client();
            $lastError = strtolower((string) ($client->getLastError() ?? ''));

            if (str_contains($lastError, 'noscript')) {
                $client->clearLastError();

                return $connection->eval(
                    $script,
                    $keyCount,
                    ...$arguments,
                );
            }
        }

        return $result;
    }

    /**
     * @param  list<string>  $keys
     * @return array<string, ?string> keyed by the original key, null when missing
     */
    public function mget(array $keys): array
    {
        if ($keys === []) {
            return [];
        }

        $connection = $this->connection();

        if ($connection instanceof PredisClusterConnection) {
            $values = [];

            foreach ($this->groupByHashTag($keys) as $group) {
                $raw = $connection->command('mget', $group);

                foreach ($group as $i => $key) {
                    $values[$key] = ($raw[$i] ?? null) !== null && ($raw[$i] ?? null) !== false
                        ? $raw[$i]
                        : null;
                }
            }

            return $values;
        }

        // PhpRedis (standalone or cluster) fans a cross-slot MGET out to the owning
        // nodes itself — only Predis's cluster client needs the manual grouping above.
        $raw = $connection->mget($keys);
        $values = [];

        foreach ($keys as $i => $key) {
            $value = $raw[$i] ?? null;
            $values[$key] = $value !== null && $value !== false ? $value : null;
        }

        return $values;
    }

    /** @param list<string> $keys */
    private function del(array $keys): void
    {
        $connection = $this->connection();

        if ($connection instanceof PredisClusterConnection) {
            foreach ($this->groupByHashTag($keys) as $group) {
                $connection->command('del', $group);
            }

            return;
        }

        if ($connection instanceof PredisConnection) {
            $connection->del($keys);

            return;
        }

        $connection->unlink($keys);
    }

    private function connection(): Connection
    {
        return $this->connection ??= Redis::connection($this->redisConnection);
    }

    /**
     * @param  list<string>  $keys
     * @return list<list<string>>
     */
    private function groupByHashTag(array $keys): array
    {
        $groups = [];

        foreach ($keys as $key) {
            if (preg_match('/\{([^{}]+)\}/', $key, $matches) === 1) {
                $groups['tag:' . $matches[1]][] = $key;
            } else {
                $groups['key:' . $key][] = $key;
            }
        }

        return array_values($groups);
    }
}
