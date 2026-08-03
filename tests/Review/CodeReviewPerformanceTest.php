<?php

namespace NormCache\Tests\Review;

use Illuminate\Redis\Connections\PhpRedisConnection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Planning\PrimaryKeyResolver;
use NormCache\Planning\SchemaRepository;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\CacheSerializer;
use NormCache\Support\QueryIdentity;
use NormCache\Support\RedisStore;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Values\CacheConfig;
use NormCache\Values\TableIdentity;
use Psr\Log\LoggerInterface;

final class CodeReviewPerformanceTest extends ReviewBenchmarkCase
{
    public function test_source_scoped_table_identity_cost(): void
    {
        $iterations = 100_000;
        $currentFields = ['nc-table', 'mysql', 'app', 'app', '', 'posts'];
        $candidateFields = ['nc-table-v2', 'shard-a', 'mysql', 'app', 'app', '', 'posts'];
        $current = static fn(): string => hash(
            'xxh128',
            TableIdentity::encodeFields($currentFields),
        );
        $candidate = static fn(): string => hash(
            'xxh128',
            TableIdentity::encodeFields($candidateFields),
        );
        $shardB = hash('xxh128', TableIdentity::encodeFields([
            'nc-table-v2',
            'shard-b',
            'mysql',
            'app',
            'app',
            '',
            'posts',
        ]));

        $this->assertNotSame($candidate(), $shardB);
        $this->reportComparison(
            'Source-scoped table identity',
            $this->bestOf(5, $iterations, $current),
            $this->bestOf(5, $iterations, $candidate),
            [
                'encoded bytes current' => strlen(TableIdentity::encodeFields($currentFields)),
                'encoded bytes candidate' => strlen(TableIdentity::encodeFields($candidateFields)),
                'production LOC change' => '+1 encoded field',
            ],
        );
    }

    public function test_schema_metadata_batching_and_model_key_candidate(): void
    {
        $connection = DB::connection();
        $modelQuery = Post::query()->toBase();
        $repository = $this->schemaRepository();
        $table = (new TableIdentityResolver($repository))->resolve($connection, 'posts');

        $this->assertNotNull($table);
        $this->assertNotNull((new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $repository,
        ))->resolve($modelQuery, $connection, $table));

        $store = $this->app->make(RedisStore::class);
        $keys = $this->app->make(CacheKeyBuilder::class);
        $connectionName = (string) $connection->getName();
        $scope = implode("\0", [
            (string) $connection->getDriverName(),
            (string) $connection->getDatabaseName(),
            (string) $connection->getTablePrefix(),
        ]);
        $viewField = 'views:' . hash('xxh128', $scope . "\0" . $table->schema);
        $primaryKeyField = 'primary-key:' . $table->hash;
        $currentSnapshot = function () use (
            $store,
            $keys,
            $connectionName,
            $viewField,
            $primaryKeyField,
        ): array {
            $epoch = $store->getRaw($keys->schemaEpoch()) ?? '0';
            $connectionEpoch = $store->getRaw(
                $keys->connectionSchemaEpoch($connectionName),
            ) ?? '0';
            $metadataKey = $keys->schema($connectionName, $epoch, $connectionEpoch);

            return [
                $store->readSchemaField($metadataKey, $viewField),
                $store->readSchemaField($metadataKey, $primaryKeyField),
            ];
        };
        $batchedSnapshot = function () use (
            $store,
            $keys,
            $connectionName,
            $viewField,
            $primaryKeyField,
        ): array {
            $epochKey = $keys->schemaEpoch();
            $connectionEpochKey = $keys->connectionSchemaEpoch($connectionName);
            $epochs = $store->mget([$epochKey, $connectionEpochKey]);
            $metadataKey = $keys->schema(
                $connectionName,
                $epochs[$epochKey] ?? '0',
                $epochs[$connectionEpochKey] ?? '0',
            );

            return $this->rawHmget($metadataKey, [$viewField, $primaryKeyField]);
        };

        $this->assertSame($currentSnapshot(), $batchedSnapshot());
        $this->reportComparison(
            'Schema metadata: four calls versus MGET + HMGET',
            $this->bestOf(5, 1000, $currentSnapshot),
            $this->bestOf(5, 1000, $batchedSnapshot),
            [
                'current Redis round trips' => 4,
                'candidate Redis round trips' => 2,
            ],
        );

        $persistentRepository = $this->schemaRepository();
        $persistentRepository->primaryKey($table);
        $currentPrimaryKey = fn() => (new PrimaryKeyResolver(
            $this->app->make(CacheConfig::class),
            $this->app->make(LoggerInterface::class),
            $persistentRepository,
        ))->resolve($modelQuery, $connection, $table);
        $trustedPrimaryKey = static fn() => $modelQuery->primaryKey();

        $this->assertEquals($currentPrimaryKey(), $trustedPrimaryKey());
        $this->reportComparison(
            'Persisted primary-key verification versus model metadata',
            $this->bestOf(5, 5000, $currentPrimaryKey),
            $this->bestOf(5, 5000, $trustedPrimaryKey),
            [
                'current first-table reads per scope' => 1,
                'candidate first-table reads per scope' => 0,
                'safety qualification' => 'review test proves model metadata can conflict',
            ],
        );
    }

    public function test_atomic_lease_claim_and_combined_fetch_claim_candidates(): void
    {
        $store = $this->app->make(RedisStore::class);
        $key = 'test:{review:claim}:build';
        $owner = str_repeat('a', 32);
        $candidateToken = str_repeat('b', 32);
        $store->setRawForever($key, $owner);
        $claimScript = <<<'LUA'
if redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX') then
    return {1, ARGV[1]}
end

return {0, redis.call('GET', KEYS[1]) or ''}
LUA;
        $claimSha = $this->loadScript($claimScript);
        $currentClaim = fn(): array => $store->setNxEx($key, $candidateToken, 30)
            ? [1, $candidateToken]
            : [0, $store->getRaw($key)];
        $atomicClaim = fn(): array => (array) $this->evalSha(
            $claimSha,
            [$key],
            [$candidateToken, '30'],
        );

        $this->assertEquals($currentClaim(), $atomicClaim());
        $this->reportComparison(
            'Waiter lease claim: SET NX + GET versus one Lua call',
            $this->bestOf(5, 5000, $currentClaim),
            $this->bestOf(5, 5000, $atomicClaim),
            [
                'current client round trips' => 2,
                'candidate client round trips' => 1,
                'candidate Lua nonblank LOC' => $this->nonBlankLines($claimScript),
            ],
        );

        $table = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), 'posts');
        $this->assertNotNull($table);
        $keys = $this->app->make(CacheKeyBuilder::class);
        $combinedScript = <<<'LUA'
local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'
local membership = redis.call(
    'GET',
    KEYS[3] .. ':m:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
)

if membership then
    return {'hit', version, generation, membership}
end

if redis.call('SET', KEYS[4], ARGV[3], 'EX', ARGV[4], 'NX') then
    return {'owner', version, generation, ARGV[3]}
end

return {'waiter', version, generation, redis.call('GET', KEYS[4]) or ''}
LUA;
        $combinedSha = $this->loadScript($combinedScript);
        $current = $this->bestCombinedMissClaim(
            repetitions: 5,
            operations: 1000,
            candidate: false,
            table: $table,
            keys: $keys,
            combinedSha: $combinedSha,
        );
        $candidate = $this->bestCombinedMissClaim(
            repetitions: 5,
            operations: 1000,
            candidate: true,
            table: $table,
            keys: $keys,
            combinedSha: $combinedSha,
        );

        $this->reportComparison(
            'Cold canonical miss: fetch then claim versus combined Lua',
            $current,
            $candidate,
            [
                'current client round trips' => 2,
                'candidate client round trips' => 1,
                'candidate Lua nonblank LOC' => $this->nonBlankLines($combinedScript),
            ],
        );
    }

    public function test_positional_canonical_mapping_candidate(): void
    {
        $prefix = 'test:{nc:t:review}:r:g0:';
        $savings = [];

        fwrite(STDOUT, "\nCanonical row mapping: associative versus positional\n");
        fwrite(STDOUT, sprintf(
            "%8s %14s %14s %12s\n",
            'rows',
            'current us',
            'candidate us',
            'saving',
        ));

        foreach ([100, 1000, 5000] as $rows) {
            $tokens = [];
            $raw = [];

            for ($index = 0; $index < $rows; $index++) {
                $tokens[] = 'i:' . $index;
                $raw[] = 'payload-' . $index;
            }

            $current = static function () use ($tokens, $raw, $prefix): int {
                $values = [];

                foreach ($tokens as $index => $token) {
                    $values[$prefix . $token] = $raw[$index];
                }

                $seen = 0;

                foreach ($tokens as $token) {
                    $seen += isset($values[$prefix . $token]) ? 1 : 0;
                }

                return $seen;
            };
            $candidate = static function () use ($tokens, $raw): int {
                $positions = [];

                foreach ($tokens as $token) {
                    $positions[$token] ??= count($positions);
                }

                $seen = 0;

                foreach ($tokens as $token) {
                    $seen += isset($raw[$positions[$token]]) ? 1 : 0;
                }

                return $seen;
            };

            $this->assertSame($current(), $candidate());

            $currentUs = $this->bestOf(5, 2000, $current);
            $candidateUs = $this->bestOf(5, 2000, $candidate);
            $savings[$rows] = $currentUs - $candidateUs;

            fwrite(STDOUT, sprintf(
                "%8d %14.2f %14.2f %12.2f\n",
                $rows,
                $currentUs,
                $candidateUs,
                $savings[$rows],
            ));
        }

        $author = Author::query()->create(['name' => 'Mapping benchmark']);
        $timestamp = now();
        $rows = [];

        for ($index = 0; $index < 1000; $index++) {
            $rows[] = [
                'title' => 'Post ' . $index,
                'views' => $index,
                'published' => true,
                'author_id' => $author->getKey(),
                'created_at' => $timestamp,
                'updated_at' => $timestamp,
            ];
        }

        foreach (array_chunk($rows, 200) as $chunk) {
            DB::table('posts')->insert($chunk);
        }

        Redis::connection('normcache-test')->flushdb();
        $this->app->forgetScopedInstances();
        $query = static fn() => Post::query()->where('published', true)->get();
        $this->assertCount(1000, $query());

        for ($index = 0; $index < 20; $index++) {
            $query();
        }

        $warmUs = $this->bestOf(5, 200, $query);
        $share = $warmUs > 0 ? $savings[1000] / $warmUs * 100 : 0;

        fwrite(STDOUT, sprintf(
            "1000-row end-to-end warm read: %.1f us; mapping saving %.2f us (%.1f%%)\n\n",
            $warmUs,
            $savings[1000],
            $share,
        ));
    }

    public function test_streaming_query_identity_candidate(): void
    {
        $identity = new QueryIdentity;
        $currentLoc = $this->methodLoc(QueryIdentity::class, 'hash');
        $candidateLoc = $this->methodLoc(self::class, 'streamingQueryHash');

        fwrite(STDOUT, "\nQuery identity: current allocation versus top-level streaming\n");
        fwrite(STDOUT, sprintf(
            "%10s %14s %14s %12s\n",
            'bindings',
            'current us',
            'candidate us',
            'change',
        ));

        foreach ([1000 => 1000, 5000 => 250, 20_000 => 50] as $count => $iterations) {
            $bindings = range(1, $count);
            $sql = 'select * from posts where id in ('
                . implode(', ', array_fill(0, $count, '?'))
                . ')';
            $arguments = [
                'canonical',
                'abcdef0123456789',
                ['dependency-b', 'dependency-a'],
                $sql,
                $bindings,
                'u',
                'select',
            ];
            $current = static fn(): string => $identity->hash(...$arguments);
            $candidate = fn(): string => $this->streamingQueryHash(...$arguments);

            $this->assertSame($current(), $candidate());

            $currentUs = $this->bestOf(5, $iterations, $current);
            $candidateUs = $this->bestOf(5, $iterations, $candidate);

            fwrite(STDOUT, sprintf(
                "%10d %14.2f %14.2f %+11.1f%%\n",
                $count,
                $currentUs,
                $candidateUs,
                $currentUs > 0 ? ($candidateUs - $currentUs) / $currentUs * 100 : 0,
            ));
        }

        fwrite(STDOUT, sprintf(
            "Prototype LOC: current %d, candidate %d, delta %+d\n\n",
            $currentLoc,
            $candidateLoc,
            $candidateLoc - $currentLoc,
        ));
    }

    public function test_list_based_canonical_publication_candidate(): void
    {
        $currentLoc = $this->methodLoc(self::class, 'currentPublicationArrays');
        $candidateLoc = $this->methodLoc(self::class, 'listPublicationArrays');

        fwrite(STDOUT, "\nCanonical publication arrays: associative versus packed lists\n");
        fwrite(STDOUT, sprintf(
            "%8s %12s %12s %11s %14s %14s %11s\n",
            'rows',
            'current us',
            'candidate us',
            'time change',
            'current peak',
            'candidate peak',
            'mem change',
        ));

        foreach ([1000 => 500, 5000 => 100, 20_000 => 20] as $rows => $iterations) {
            $tokens = [];
            $payloads = [];

            for ($index = 0; $index < $rows; $index++) {
                $tokens[] = 'i:' . $index;
                $payloads[] = str_repeat(chr(65 + $index % 26), 128) . $index;
            }

            $current = fn(): array => $this->currentPublicationArrays($tokens, $payloads);
            $candidate = fn(): array => $this->listPublicationArrays($tokens, $payloads);
            $currentResult = $current();
            $candidateResult = $candidate();

            $this->assertSame($currentResult['keys'], $candidateResult['keys']);
            $this->assertSame($currentResult['args'], $candidateResult['args']);

            unset($currentResult, $candidateResult);

            $currentUs = $this->bestOf(5, $iterations, $current);
            $candidateUs = $this->bestOf(5, $iterations, $candidate);
            $currentPeak = $this->peakMemory($current);
            $candidatePeak = $this->peakMemory($candidate);

            fwrite(STDOUT, sprintf(
                "%8d %12.2f %12.2f %+10.1f%% %11.1f KiB %11.1f KiB %+10.1f%%\n",
                $rows,
                $currentUs,
                $candidateUs,
                $currentUs > 0 ? ($candidateUs - $currentUs) / $currentUs * 100 : 0,
                $currentPeak / 1024,
                $candidatePeak / 1024,
                $currentPeak > 0 ? ($candidatePeak - $currentPeak) / $currentPeak * 100 : 0,
            ));
        }

        fwrite(STDOUT, sprintf(
            "Prototype LOC: current %d, candidate %d, delta %+d\n\n",
            $currentLoc,
            $candidateLoc,
            $candidateLoc - $currentLoc,
        ));
    }

    public function test_self_describing_serializer_candidate(): void
    {
        $php = new CacheSerializer(false);
        $igbinary = extension_loaded('igbinary') ? new CacheSerializer(true) : null;
        $writer = $igbinary ?? $php;
        $prefix = $igbinary === null ? 'P' : 'I';
        $value = [];

        for ($index = 0; $index < 100; $index++) {
            $value[] = [
                'id' => $index,
                'title' => 'Row ' . $index,
                'payload' => str_repeat(chr(65 + $index % 26), 256),
            ];
        }

        $encoded = $writer->encode($value);
        $tagged = $prefix . $encoded;
        $current = static fn(): mixed => $writer->decode($encoded);
        $candidate = fn(): mixed => $this->decodeTagged($tagged, $php, $igbinary);

        $this->assertSame($value, $current());
        $this->assertSame($value, $candidate());
        $this->assertSame($value, $this->decodeTagged('P' . $php->encode($value), $php, $igbinary));
        $this->reportComparison(
            'Self-describing serializer prefix',
            $this->bestOf(5, 5000, $current),
            $this->bestOf(5, 5000, $candidate),
            [
                'payload bytes current' => strlen($encoded),
                'payload bytes candidate' => strlen($tagged),
                'current decode LOC' => $this->methodLoc(CacheSerializer::class, 'decode'),
                'candidate decode LOC' => $this->methodLoc(self::class, 'decodeTagged'),
            ],
        );
    }

    public function test_uncertain_write_catch_normal_path_cost(): void
    {
        $operation = static fn(): int => 1;
        $current = fn(): int => $this->currentWriteWrapper($operation);
        $candidate = fn(): int => $this->uncertainSafeWriteWrapper($operation);

        $this->assertSame($current(), $candidate());
        $this->reportComparison(
            'Normal-path cost of adding an uncertain-write catch',
            $this->bestOf(5, 1_000_000, $current),
            $this->bestOf(5, 1_000_000, $candidate),
            [
                'current prototype LOC' => $this->methodLoc(self::class, 'currentWriteWrapper'),
                'candidate prototype LOC' => $this->methodLoc(self::class, 'uncertainSafeWriteWrapper'),
            ],
        );
    }

    private function schemaRepository(): SchemaRepository
    {
        return new SchemaRepository(
            $this->app->make(CacheConfig::class),
            $this->app->make(RedisStore::class),
            $this->app->make(CacheKeyBuilder::class),
        );
    }

    /** @param list<string> $fields
     * @return list<?string>
     */
    private function rawHmget(string $key, array $fields): array
    {
        $connection = Redis::connection('normcache-test');
        $raw = $connection instanceof PhpRedisConnection
            ? $connection->withoutSerializationOrCompression(
                static fn(): mixed => $connection->client()->hMGet($key, $fields),
            )
            : $connection->command('hmget', [$key, ...$fields]);

        if (!is_array($raw)) {
            throw new \UnexpectedValueException('Redis HMGET must return an array.');
        }

        $values = [];

        foreach ($fields as $index => $field) {
            $value = array_key_exists($field, $raw) ? $raw[$field] : ($raw[$index] ?? null);
            $values[] = is_string($value) ? $value : null;
        }

        return $values;
    }

    private function loadScript(string $script): string
    {
        $sha = Redis::connection('normcache-test')->command('script', ['load', $script]);

        if (!is_string($sha)) {
            throw new \UnexpectedValueException('Redis SCRIPT LOAD must return a SHA.');
        }

        return $sha;
    }

    /** @param list<string> $keys
     * @param  list<string>  $arguments
     */
    private function evalSha(string $sha, array $keys, array $arguments): mixed
    {
        $connection = Redis::connection('normcache-test');
        $parameters = [...$keys, ...$arguments];

        return $connection instanceof PhpRedisConnection
            ? $connection->withoutSerializationOrCompression(
                static fn(): mixed => $connection->client()->evalSha(
                    $sha,
                    $parameters,
                    count($keys),
                ),
            )
            : $connection->command('evalsha', [$sha, count($keys), ...$parameters]);
    }

    private function bestCombinedMissClaim(
        int $repetitions,
        int $operations,
        bool $candidate,
        TableIdentity $table,
        CacheKeyBuilder $keys,
        string $combinedSha,
    ): float {
        $best = INF;
        $store = $this->app->make(RedisStore::class);
        $versionKey = $keys->version($table);
        $generationKey = $keys->generation($table);
        $tablePrefix = $keys->tablePrefix($table);

        for ($repetition = 0; $repetition < $repetitions; $repetition++) {
            $items = [];

            for ($index = 0; $index < $operations; $index++) {
                $queryHash = hash('xxh128', implode(':', [
                    $candidate ? 'candidate' : 'current',
                    (string) $repetition,
                    (string) $index,
                    bin2hex(random_bytes(4)),
                ]));
                $items[] = [
                    $queryHash,
                    $keys->membershipBuild($table, '0', 'u', $queryHash),
                    hash('xxh128', 'token:' . $queryHash),
                ];
            }

            $started = hrtime(true);

            foreach ($items as [$queryHash, $buildingKey, $token]) {
                if ($candidate) {
                    $this->evalSha(
                        $combinedSha,
                        [$versionKey, $generationKey, $tablePrefix, $buildingKey],
                        ['u', $queryHash, $token, '30'],
                    );

                    continue;
                }

                $store->fetchCanonical(
                    $versionKey,
                    $generationKey,
                    $tablePrefix,
                    'u',
                    $queryHash,
                );
                $store->setNxEx($buildingKey, $token, 30);
            }

            $best = min($best, ((hrtime(true) - $started) / 1000) / $operations);
        }

        return $best;
    }

    /**
     * @param  list<string>  $dependencyHashes
     * @param  list<mixed>  $bindings
     */
    private function streamingQueryHash(
        string $route,
        string $rootHash,
        array $dependencyHashes,
        string $sql,
        array $bindings,
        string $namespace,
        string $operation,
    ): string {
        if (count($dependencyHashes) > 1) {
            $dependencyHashes = array_values(array_unique($dependencyHashes));
            sort($dependencyHashes, SORT_STRING);
        }

        $prepared = '';

        foreach ($bindings as $binding) {
            $prepared .= $this->reviewBinding($binding);
        }

        $context = hash_init('xxh128');

        foreach ([
            'nc-query',
            $route,
            $rootHash,
            TableIdentity::encodeFields($dependencyHashes),
            $sql,
            $prepared,
            $namespace,
            $operation,
        ] as $field) {
            hash_update($context, strlen($field) . ':');
            hash_update($context, $field);
        }

        return hash_final($context);
    }

    private function reviewBinding(mixed $value): string
    {
        if ($value instanceof \BackedEnum) {
            $value = $value->value;
        } elseif ($value instanceof \UnitEnum) {
            $value = $value->name;
        } elseif ($value instanceof \DateTimeInterface) {
            $value = $value->format('Y-m-d H:i:s.uP');
        } elseif ($value instanceof \Stringable) {
            $value = (string) $value;
        }

        return match (true) {
            $value === null => '4:null0:',
            is_bool($value) => $value ? '4:bool1:1' : '4:bool1:0',
            is_int($value) => '3:int' . strlen($digits = (string) $value) . ':' . $digits,
            is_float($value) => '5:float8:' . pack('E', $value),
            is_string($value) => '6:string' . strlen($value) . ':' . $value,
            default => throw new \InvalidArgumentException('Unsupported review binding.'),
        };
    }

    /** @param list<string> $tokens
     * @param  list<string>  $payloads
     * @return array{keys: list<string>, args: list<string>}
     */
    private function currentPublicationArrays(array $tokens, array $payloads): array
    {
        $entries = [];

        foreach ($tokens as $index => $token) {
            $entries['test:{nc:t:review}:r:g0:' . $token] = $payloads[$index];
        }

        return [
            'keys' => ['version', 'generation', 'membership', ...array_keys($entries), 'build', 'wake'],
            'args' => ['count', 'version', 'generation', 'ttl', ...array_values($entries), 'token'],
        ];
    }

    /** @param list<string> $tokens
     * @param  list<string>  $payloads
     * @return array{keys: list<string>, args: list<string>}
     */
    private function listPublicationArrays(array $tokens, array $payloads): array
    {
        $rowKeys = [];
        $rowPayloads = [];

        foreach ($tokens as $index => $token) {
            $rowKeys[] = 'test:{nc:t:review}:r:g0:' . $token;
            $rowPayloads[] = $payloads[$index];
        }

        return [
            'keys' => ['version', 'generation', 'membership', ...$rowKeys, 'build', 'wake'],
            'args' => ['count', 'version', 'generation', 'ttl', ...$rowPayloads, 'token'],
        ];
    }

    private function decodeTagged(
        string $payload,
        CacheSerializer $php,
        ?CacheSerializer $igbinary,
    ): mixed {
        return match ($payload[0] ?? '') {
            'P' => $php->decode(substr($payload, 1)),
            'I' => $igbinary?->decode(substr($payload, 1)),
            default => null,
        };
    }

    private function currentWriteWrapper(callable $operation): mixed
    {
        try {
            return $operation();
        } finally {
            // Existing owner cleanup would run here.
        }
    }

    private function uncertainSafeWriteWrapper(callable $operation): mixed
    {
        try {
            return $operation();
        } catch (\Throwable $exception) {
            // A production implementation would conservatively invalidate here.
            throw $exception;
        } finally {
            // Existing owner cleanup would run here.
        }
    }

    private function bestOf(int $repetitions, int $iterations, callable $operation): float
    {
        $operation();
        $best = INF;

        for ($repetition = 0; $repetition < $repetitions; $repetition++) {
            $started = hrtime(true);

            for ($iteration = 0; $iteration < $iterations; $iteration++) {
                $operation();
            }

            $best = min($best, ((hrtime(true) - $started) / 1000) / $iterations);
        }

        return $best;
    }

    private function peakMemory(callable $operation): int
    {
        gc_collect_cycles();
        memory_reset_peak_usage();
        $before = memory_get_usage(false);
        $result = $operation();
        $peak = memory_get_peak_usage(false) - $before;
        unset($result);
        gc_collect_cycles();

        return max(0, $peak);
    }

    private function methodLoc(string $class, string $method): int
    {
        $reflection = new \ReflectionMethod($class, $method);

        return $reflection->getEndLine() - $reflection->getStartLine() + 1;
    }

    private function nonBlankLines(string $source): int
    {
        return count(array_filter(
            explode("\n", $source),
            static fn(string $line): bool => trim($line) !== '',
        ));
    }

    /** @param array<string, int|string> $details */
    private function reportComparison(
        string $label,
        float $currentUs,
        float $candidateUs,
        array $details = [],
    ): void {
        fwrite(STDOUT, "\n{$label}\n");
        fwrite(STDOUT, sprintf("  current:   %10.3f us/op\n", $currentUs));
        fwrite(STDOUT, sprintf("  candidate: %10.3f us/op\n", $candidateUs));
        fwrite(STDOUT, sprintf(
            "  change:    %+9.1f%% (%0.2fx current/candidate)\n",
            $currentUs > 0 ? ($candidateUs - $currentUs) / $currentUs * 100 : 0,
            $candidateUs > 0 ? $currentUs / $candidateUs : 0,
        ));

        foreach ($details as $name => $value) {
            fwrite(STDOUT, "  {$name}: {$value}\n");
        }

        fwrite(STDOUT, "\n");
    }
}
