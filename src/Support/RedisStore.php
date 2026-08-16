<?php

namespace NormCache\Support;

use Illuminate\Redis\Connections\Connection;
use Illuminate\Redis\Connections\PhpRedisClusterConnection;
use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Redis\Connections\PredisClusterConnection;
use Illuminate\Redis\Connections\PredisConnection;
use Illuminate\Support\Facades\Redis;
use NormCache\Exceptions\TableInvalidationException;
use Predis\NotSupportedException;
use Predis\Response\ServerException;

final class RedisStore
{
    private const WAKE_TOKENS = 64;

    // Redis runs a script atomically on its single thread, so one call must never
    // carry an unbounded row batch. Narrow rows are bound by count and wide rows
    // by size; both limits are measured to stall near two milliseconds, and a
    // slice ends at whichever is reached first.
    private const ROW_PUBLISH_CHUNK = 250;

    private const ROW_PUBLISH_CHUNK_BYTES = 1_048_576;

    private ?Connection $connection = null;

    /** @var array<string, string> */
    private static array $shas = [];

    public function __construct(
        private string $redisConnection,
    ) {}

    public function getRaw(string $key): ?string
    {
        return $this->withRawValues(static function (Connection $connection) use ($key): ?string {
            $value = $connection->get($key);

            return $value !== null && $value !== false ? $value : null;
        });
    }

    public function readHashField(string $key, string $field): ?string
    {
        return $this->withRawValues(static function (Connection $connection) use ($key, $field): ?string {
            $value = $connection->hget($key, $field);

            return is_string($value) ? $value : null;
        });
    }

    public function readHashFieldWithValues(string $key, string $field, array $valueKeys): array
    {
        $connection = $this->connection();

        if (
            $valueKeys === []
            || $connection instanceof PredisClusterConnection
            || $connection instanceof PhpRedisClusterConnection
        ) {
            return [$this->readHashField($key, $field), $this->mget($valueKeys)];
        }

        return $this->withRawValues(function (Connection $connection) use ($key, $field, $valueKeys): array {
            $queue = static function (mixed $pipe) use ($key, $field, $valueKeys): void {
                $pipe->hget($key, $field);
                $pipe->mget($valueKeys);
            };

            // Predis accepts the callback directly; phpredis needs Laravel's wrapper.
            $replies = (array) ($connection instanceof PhpRedisConnection
                ? $connection->pipeline($queue)
                : $connection->command('pipeline', [$queue]));

            return [
                is_string($replies[0] ?? null) ? $replies[0] : null,
                $this->mapMgetValues($valueKeys, $replies[1] ?? []),
            ];
        });
    }

    public function writeHashField(string $key, string $field, string $value): void
    {
        $this->withRawValues(static function (Connection $connection) use ($key, $field, $value): void {
            $connection->hset($key, $field, $value);
        });
    }

    public function deleteHashField(string $key, string $field): void
    {
        $this->withRawValues(static function (Connection $connection) use ($key, $field): void {
            $connection->hdel($key, $field);
        });
    }

    public function setRawForever(string $key, string $value): void
    {
        $this->withRawValues(static function (Connection $connection) use ($key, $value): void {
            $connection->set($key, $value);
        });
    }

    /** @return array{0: bool, 1: ?string} */
    public function claimBuild(string $key, string $token, int $ttl): array
    {
        $result = (array) $this->script(
            RedisScripts::get('claim_build'),
            [$key],
            [$token, (string) $ttl],
        );
        $owner = is_string($result[1] ?? null) && $result[1] !== ''
            ? $result[1]
            : null;

        return [(int) ($result[0] ?? 0) === 1, $owner];
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
            entryKeys: [],
            entryPayloads: [],
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
     * @param  list<string>  $entryKeys
     * @param  list<string>  $entryPayloads
     * @param  list<string>  $versionKeys
     * @param  list<string>  $expectedVersions
     * @param  list<string>  $entryFields  hash field per entry; an empty field writes a
     *                                     plain string entry instead
     */
    public function publishVersionedEntries(
        array $entryKeys,
        array $entryPayloads,
        int $ttl,
        array $versionKeys,
        array $expectedVersions,
        ?string $buildingKey = null,
        ?string $wakeKey = null,
        ?string $token = null,
        int $wakeTtl = 10,
        array $entryFields = [],
    ): bool {
        if (count($entryKeys) !== count($entryPayloads)) {
            throw new \InvalidArgumentException(
                'NormCache versioned entry keys and payloads must have the same length.',
            );
        }

        if ($entryFields === []) {
            $entryFields = array_fill(0, count($entryKeys), '');
        } elseif (count($entryFields) !== count($entryKeys)) {
            throw new \InvalidArgumentException(
                'NormCache versioned entry keys and fields must have the same length.',
            );
        }

        $keys = [...$versionKeys, ...$entryKeys];

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
                (string) count($entryKeys),
                (string) $ttl,
                ...$expectedVersions,
                ...$entryFields,
                ...$entryPayloads,
                $token ?? '',
                (string) self::WAKE_TOKENS,
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

    /**
     * @param  list<string>  $rowKeys
     * @param  list<string>  $rowPayloads
     */
    public function publishRows(
        string $versionKey,
        string $generationKey,
        string $buildingKey,
        array $rowKeys,
        array $rowPayloads,
        string $expectedVersion,
        string $expectedGeneration,
        int $rowTtl,
        string $token,
        int $leaseTtl,
    ): bool {
        if (count($rowKeys) !== count($rowPayloads)) {
            throw new \InvalidArgumentException(
                'NormCache canonical row keys and payloads must have the same length.',
            );
        }

        foreach ($this->rowSlices($rowKeys, $rowPayloads) as [$keys, $payloads]) {
            $published = (bool) $this->script(
                RedisScripts::get('publish_rows'),
                [$versionKey, $generationKey, $buildingKey, ...$keys],
                [
                    $expectedVersion,
                    $expectedGeneration,
                    (string) $rowTtl,
                    $token,
                    (string) $leaseTtl,
                    ...$payloads,
                ],
            );

            if (!$published) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param  list<string>  $rowKeys
     * @param  list<string>  $rowPayloads
     * @return list<array{0: list<string>, 1: list<string>}>
     */
    private function rowSlices(array $rowKeys, array $rowPayloads): array
    {
        $slices = [];
        $keys = [];
        $payloads = [];
        $bytes = 0;

        foreach ($rowKeys as $index => $key) {
            $payload = $rowPayloads[$index];

            // Never emit an empty slice, so an oversized row still publishes alone.
            if (
                $keys !== []
                && (count($keys) >= self::ROW_PUBLISH_CHUNK
                    || $bytes + strlen($payload) > self::ROW_PUBLISH_CHUNK_BYTES)
            ) {
                $slices[] = [$keys, $payloads];
                $keys = [];
                $payloads = [];
                $bytes = 0;
            }

            $keys[] = $key;
            $payloads[] = $payload;
            $bytes += strlen($payload);
        }

        if ($keys !== []) {
            $slices[] = [$keys, $payloads];
        }

        return $slices;
    }

    /**
     * @param  list<string>  $rowKeys
     * @param  list<string>  $rowPayloads
     */
    public function publishCanonical(
        string $versionKey,
        string $generationKey,
        string $membershipKey,
        array $rowKeys,
        array $rowPayloads,
        string $expectedVersion,
        string $expectedGeneration,
        string $membershipPayload,
        int $membershipTtl,
        int $rowTtl,
        string $buildingKey,
        string $wakeKey,
        string $token,
        int $wakeTtl,
        ?string $resultPayload = null,
    ): bool {
        if (count($rowKeys) !== count($rowPayloads)) {
            throw new \InvalidArgumentException(
                'NormCache canonical row keys and payloads must have the same length.',
            );
        }

        $args = [
            (string) count($rowKeys),
            $expectedVersion,
            $expectedGeneration,
            (string) $membershipTtl,
            (string) $rowTtl,
            $membershipPayload,
            ...$rowPayloads,
            $token,
            (string) self::WAKE_TOKENS,
            (string) $wakeTtl,
        ];

        if ($resultPayload !== null) {
            $args[] = $resultPayload;
        }

        return (bool) $this->script(
            RedisScripts::get('publish_canonical'),
            [
                $versionKey,
                $generationKey,
                $membershipKey,
                ...$rowKeys,
                $buildingKey,
                $wakeKey,
            ],
            $args,
        );
    }

    public function increment(string $key): int
    {
        return (int) $this->withRetryingConnection(
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
        string $changePrefix,
        string $changePayload,
        int $changeTtl,
    ): ?string {
        $reply = $this->script(
            RedisScripts::get('invalidate_table'),
            [$versionKey, $generationKey, $rowPrefix, $changePrefix],
            [
                $mode,
                $changePayload,
                (string) $changeTtl,
                ...$tokens,
            ],
        );

        return $this->replyVersion($reply);
    }

    private function replyVersion(mixed $reply): ?string
    {
        $version = is_array($reply) ? ($reply[0] ?? null) : null;

        return is_int($version) || is_string($version) ? (string) $version : null;
    }

    /**
     * @param  list<array{
     *     versionKey: string,
     *     generationKey: string,
     *     mode: string,
     *     tokens: list<string>,
     *     rowPrefix: string,
     *     changePrefix: string,
     *     changePayload: string,
     *     changeTtl: int
     * }>  $states
     * @return list<?string> the new version of each state, index-aligned with $states
     */
    public function invalidateTableStates(array $states): array
    {
        if (count($states) === 1) {
            return [$this->invalidateTableState(...$states[0])];
        }

        return (array) $this->withRetryingConnection(function (Connection $connection) use ($states): array {
            if (
                $connection instanceof PhpRedisClusterConnection
                || $connection instanceof PredisClusterConnection
            ) {
                $versions = [];

                foreach ($states as $index => $state) {
                    try {
                        $versions[] = $this->replyVersion($this->evaluate(
                            $connection,
                            RedisScripts::get('invalidate_table'),
                            [
                                $state['versionKey'],
                                $state['generationKey'],
                                $state['rowPrefix'],
                                $state['changePrefix'],
                            ],
                            [
                                $state['mode'],
                                $state['changePayload'],
                                (string) $state['changeTtl'],
                                ...$state['tokens'],
                            ],
                        ));
                    } catch (\Exception $exception) {
                        throw new TableInvalidationException($index, $exception);
                    }
                }

                return $versions;
            }

            $keys = [];
            $args = [];

            foreach ($states as $state) {
                $keys[] = $state['versionKey'];
                $keys[] = $state['generationKey'];
                $keys[] = $state['rowPrefix'];
                $keys[] = $state['changePrefix'];
                $args[] = $state['mode'];
                $args[] = (string) count($state['tokens']);
                $args[] = $state['changePayload'];
                $args[] = (string) $state['changeTtl'];
                array_push($args, ...$state['tokens']);
            }

            $reply = (array) $this->evaluate(
                $connection,
                RedisScripts::get('invalidate_tables'),
                $keys,
                $args,
            );

            return array_map(
                static fn(mixed $version): ?string => is_int($version) || is_string($version)
                    ? (string) $version
                    : null,
                array_values($reply),
            );
        });
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
        }, retry: false);
    }

    /**
     * @param  list<string>  $keys
     * @param  list<mixed>  $args
     */
    private function script(string $script, array $keys, array $args = []): mixed
    {
        return $this->withRetryingConnection(
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
     * @return array<string, ?string>
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

            // PhpRedis handles cross-slot MGET fan-out itself.
            return $this->mapMgetValues($keys, $connection->mget($keys));
        });
    }

    /** @param list<string> $keys */
    private function del(array $keys): void
    {
        $this->withRetryingConnection(function (Connection $connection) use ($keys): void {
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

    private function withRawValues(callable $callback, bool $retry = true): mixed
    {
        $operation = static function (Connection $connection) use ($callback): mixed {
            if ($connection instanceof PhpRedisConnection) {
                return $connection->withoutSerializationOrCompression(
                    static fn(): mixed => $callback($connection),
                );
            }

            return $callback($connection);
        };

        return $retry
            ? $this->withRetryingConnection($operation)
            : $operation($this->connection());
    }

    private function withRetryingConnection(callable $operation): mixed
    {
        try {
            return $operation($this->connection());
        } catch (\Exception) {
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
