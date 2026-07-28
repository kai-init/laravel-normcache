<?php

namespace NormCache\Tests\Unit;

use InvalidArgumentException;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheConfig;
use PHPUnit\Framework\Attributes\DataProvider;

final class CacheConfigTest extends UnitTestCase
{
    public function test_it_builds_the_v4_configuration_contract(): void
    {
        $config = CacheConfig::fromArray([
            'connection' => 'normcache-test',
            'key_prefix' => 'app:',
            'row_ttl' => 600,
            'query_ttl' => 60,
            'primary_keys' => [],
            'events' => true,
        ]);

        $this->assertSame('normcache-test', $config->connection);
        $this->assertSame('app:', $config->keyPrefix);
        $this->assertSame(600, $config->rowTtl);
        $this->assertSame(60, $config->queryTtl);
        $this->assertSame(1000, $config->maxPreciseInvalidationKeys);
        $this->assertTrue($config->dispatchEvents);
        $this->assertFalse(property_exists($config, 'cooldown'));
        $this->assertFalse(property_exists($config, 'deploymentIds'));
        $this->assertFalse(property_exists($config, 'publicationGuardMarginSeconds'));
        $this->assertFalse(property_exists($config, 'fallbackEnabled'));
    }

    public function test_it_rejects_hash_tag_characters_in_the_key_prefix(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('key_prefix');

        CacheConfig::fromArray(['key_prefix' => 'tenant:{unsafe}:']);
    }

    #[DataProvider('invalidSafetyValues')]
    public function test_it_rejects_invalid_safety_values(string $key, int $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($key);

        CacheConfig::fromArray([$key => $value]);
    }

    public static function invalidSafetyValues(): array
    {
        return [
            ['max_precise_invalidation_keys', 1001],
            ['building_lock_ttl', 0],
            ['row_ttl', 0],
            ['query_ttl', 0],
        ];
    }

    public function test_it_expands_grouped_primary_key_overrides(): void
    {
        $config = CacheConfig::fromArray([
            'primary_keys' => [[
                'connection' => 'pgsql',
                'database' => 'app',
                'schema' => 'public',
                'tables' => [
                    'events' => ['column' => 'event_id', 'type' => 'string'],
                    'orders' => ['column' => 'order_id', 'type' => 'integer'],
                ],
            ]],
        ]);

        $this->assertSame([
            [
                'connection' => 'pgsql',
                'database' => 'app',
                'table' => 'events',
                'column' => 'event_id',
                'type' => 'string',
                'schema' => 'public',
            ],
            [
                'connection' => 'pgsql',
                'database' => 'app',
                'table' => 'orders',
                'column' => 'order_id',
                'type' => 'integer',
                'schema' => 'public',
            ],
        ], $config->primaryKeys);
    }

    public function test_it_validates_grouped_primary_key_overrides(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('type must be integer or string');

        CacheConfig::fromArray([
            'primary_keys' => [[
                'connection' => 'tenant',
                'database' => 'app',
                'tables' => [
                    'events' => ['column' => 'event_id', 'type' => 'uuid'],
                ],
            ]],
        ]);
    }

    public function test_it_rejects_ungrouped_primary_key_overrides(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('groups require a non-empty tables array');

        CacheConfig::fromArray([
            'primary_keys' => [[
                'connection' => 'pgsql',
                'database' => 'app',
                'table' => 'events',
                'column' => 'event_id',
                'type' => 'string',
            ]],
        ]);
    }
}
