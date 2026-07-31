<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use Predis\NotSupportedException;
use Predis\Response\ServerException;

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
        return $this->withRawValues(static function (Connection $connection) use ($key): ?string {
            $value = $connection->get($key);

            return $value !== null && $value !== false ? $value : null;
        });
    }

    public function setRawForever(string $key, string $value): void
    {
        $this->withRawValues(static function (Connection $connection) use ($key, $value): void {
            $connection->set($key, $value);
        });
    }

    public function setRaw(string $key, string $value, int $ttl): void
    {
        $this->withRawValues(static function (Connection $connection) use ($key, $value, $ttl): void {
            $connection->setex($key, $ttl, $value);
        });
    }

    public function setNxEx(string $key, string $value, int $ttl): bool
    {
        return $this->withRawValues(static function (Connection $connection) use ($key, $value, $ttl): bool {
            if ($connection instanceof PhpRedisConnection) {
                return $connection->client()->set($key, $value, ['nx', 'ex' => $ttl]) !== false;
            }

            $result = $connection->command('set', [$key, $value, 'EX', $ttl, 'NX']);

            return $result !== null && $result !== false;
        });
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
        return $this->publishVersionedEntries(
            entries: [],
            ttl: 1,
            versionKeys: [],
            expectedVersions: [],
            buildingKey: $buildingKey,
            wakeKey: $wakeKey,
            token: $token,
            wakeTtl: $wakeTtl,
        );
    }

    /**
     * @param  array<string, string>  $entries
     * @param  list<string>  $versionKeys
     * @param  list<string>  $expectedVersions
     */
    public function publishVersionedEntries(
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
            RedisScripts::get('publish_versioned_entries'),
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
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_canonical'),
            [$versionKey, $generationKey, $tablePrefix],
            [$namespace, $queryHash],
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
    public function fetchResult(
        string $versionKey,
        string $tablePrefix,
        string $namespace,
        string $queryHash,
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_result'),
            [$versionKey, $tablePrefix],
            [$namespace, $queryHash],
        );
    }

    /** @return array<int, mixed> */
    public function fetchResultOrCanonical(
        string $versionKey,
        string $generationKey,
        string $tablePrefix,
        string $namespace,
        string $resultQueryHash,
        string $canonicalQueryHash,
    ): array {
        return (array) $this->script(
            RedisScripts::get('fetch_result_or_canonical'),
            [$versionKey, $generationKey, $tablePrefix],
            [$namespace, $resultQueryHash, $canonicalQueryHash],
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
        ?string $resultKey = null,
        ?string $resultPayload = null,
    ): bool {
        $keys = [
            $versionKey,
            $generationKey,
            $membershipKey,
            ...array_keys($rows),
            $buildingKey,
            $wakeKey,
        ];
        $args = [
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
        ];

        if ($resultKey !== null && $resultPayload !== null) {
            $keys[] = $resultKey;
            $args[] = $resultPayload;
        }

        return (bool) $this->script(RedisScripts::get('publish_canonical'), $keys, $args);
    }

    public function increment(string $key): int
    {
        return (int) $this->withConnection(
            static fn(Connection $connection): mixed => $connection->incr($key),
        );
    }

    /** @param list<string> $tokens */
    public function invalidateTableState(
        string $versionKey,
        string $generationKey,
        string $mode,
        array $tokens,
        string $rowPrefix,
    ): void {
        $this->script(
            RedisScripts::get('invalidate_table'),
            [$versionKey, $generationKey],
            [
                $mode,
                $rowPrefix,
                ...$tokens,
            ],
        );
    }

    public function enableCache(string $epochKey, string $disabledKey): int
    {
        return (int) $this->script(
            RedisScripts::get('enable_cache'),
            [$epochKey, $disabledKey],
        );
    }

    public function brpop(string $key, float $timeoutSeconds): bool
    {
        return $this->withRawValues(static function (Connection $connection) use ($key, $timeoutSeconds): bool {
            $result = $connection->brpop($key, $timeoutSeconds);

            return $result !== null && $result !== false;
        });
    }

    /**
     * All keys passed to one script must share a Redis Cluster hash slot.
     *
     * @param  list<string>  $keys
     * @param  list<mixed>  $args
     */
    private function script(string $script, array $keys, array $args = []): mixed
    {
        return $this->withConnection(
            fn(Connection $connection): mixed => $this->evaluate($connection, $script, $keys, $args),
        );
    }

    /**
     * @param  list<string>  $keys
     * @param  list<mixed>  $args
     */
    private function evaluate(
        Connection $connection,
        string $script,
        array $keys,
        array $args,
    ): mixed {
        $keyCount = count($keys);
        $arguments = [...$keys, ...$args];
        $sha = self::$shas[$script] ??= sha1($script);

        try {
            if ($connection instanceof PhpRedisConnection) {
                $result = $connection->client()->evalSha($sha, $arguments, $keyCount);
            } else {
                $result = $connection->command('evalsha', [$sha, $keyCount, ...$arguments]);
            }
        } catch (\Throwable $exception) {
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

        return $this->withRawValues(function (Connection $connection) use ($keys): array {
            if ($connection instanceof PredisClusterConnection) {
                $groups = $this->groupByHashTag($keys);

                if (count($groups) === 1) {
                    return $this->mapMgetValues($groups[0], $connection->mget($groups[0]));
                }

                try {
                    $replies = $connection->pipeline(static function ($pipeline) use ($groups): void {
                        foreach ($groups as $group) {
                            $pipeline->mget(...$group);
                        }
                    });
                } catch (ServerException $exception) {
                    if (
                        !str_starts_with($exception->getMessage(), 'MOVED ')
                        && !str_starts_with($exception->getMessage(), 'ASK ')
                    ) {
                        throw $exception;
                    }

                    $replies = array_map(
                        static fn(array $group): mixed => $connection->command('mget', $group),
                        $groups,
                    );
                }

                $values = [];

                foreach ($groups as $groupIndex => $group) {
                    $values += $this->mapMgetValues($group, $replies[$groupIndex] ?? []);
                }

                return $values;
            }

            // PhpRedis (standalone or cluster) fans a cross-slot MGET out to the owning
            // nodes itself — only Predis's cluster client needs the manual grouping above.
            return $this->mapMgetValues($keys, $connection->mget($keys));
        });
    }

    /** @param list<string> $keys */
    private function del(array $keys): void
    {
        $this->withConnection(function (Connection $connection) use ($keys): void {
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
        });
    }

    private function withRawValues(callable $callback): mixed
    {
        return $this->withConnection(static function (Connection $connection) use ($callback): mixed {
            if ($connection instanceof PhpRedisConnection) {
                return $connection->withoutSerializationOrCompression(
                    static fn(): mixed => $callback($connection),
                );
            }

            return $callback($connection);
        });
    }

    private function withConnection(callable $operation): mixed
    {
        try {
            return $operation($this->connection());
        } catch (\Throwable) {
            $this->connection = null;
            Redis::purge($this->redisConnection);

            return $operation($this->connection());
        }
    }

    private function connection(): Connection
    {
        return $this->connection ??= Redis::connection($this->redisConnection);
    }

    /**
     * @param  list<string>  $keys
     * @return array<string, ?string>
     */
    private function mapMgetValues(array $keys, mixed $raw): array
    {
        if (!is_array($raw)) {
            throw new \UnexpectedValueException('Redis MGET must return an array.');
        }

        $values = [];

        foreach ($keys as $i => $key) {
            $value = $raw[$i] ?? null;
            $values[$key] = $value !== null && $value !== false ? $value : null;
        }

        return $values;
    }

    /**
     * @param  list<string>  $keys
     * @return list<list<string>>
     */
    private function groupByHashTag(array $keys): array
    {
        $groups = [];

        foreach ($keys as $key) {
            $open = strpos($key, '{');
            $close = $open === false ? false : strpos($key, '}', $open + 1);
            $group = $close !== false && $close - $open > 1
                ? 'tag:' . substr($key, $open + 1, $close - $open - 1)
                : 'key:' . $key;

            $groups[$group][] = $key;
        }

        return array_values($groups);
    }
}
