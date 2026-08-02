<?php

namespace NormCache\Tests\Unit;

use InvalidArgumentException;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\CacheConfig;
use PHPUnit\Framework\Attributes\DataProvider;

final class CacheConfigTest extends UnitTestCase
{
    public function test_populates_default_configuration_values(): void
    {
        $config = CacheConfig::fromArray([]);

        $this->assertSame('cache', $config->connection);
        $this->assertSame('', $config->keyPrefix);
        $this->assertSame(604_800, $config->rowTtl);
        $this->assertSame(3_600, $config->queryTtl);
        $this->assertSame(1000, $config->maxAutoOverlayRows);
        $this->assertSame(1000, $config->maxPreciseInvalidationKeys);
        $this->assertSame(5, $config->buildingLockTtl);
        $this->assertSame(200, $config->stampedeWaitMs);
        $this->assertSame(64, $config->stampedeWakeTokens);
        $this->assertTrue($config->enabled);
        $this->assertFalse($config->dispatchEvents);
        $this->assertFalse($config->debugbar);
    }

    public function test_rejects_hash_tag_characters_in_key_prefix(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('key_prefix');

        CacheConfig::fromArray(['key_prefix' => 'tenant:{unsafe}:']);
    }

    #[DataProvider('invalidSafetyValues')]
    public function test_rejects_invalid_safety_values(string $key, int $value): void
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
            ['auto_overlay_max_rows', -1],
            ['stampede_wake_tokens', 0],
            ['stampede_wake_tokens', -1],
            ['stampede_wake_tokens', 1001],
        ];
    }

    public function test_accepts_the_maximum_stampede_wake_token_count(): void
    {
        $config = CacheConfig::fromArray(['stampede_wake_tokens' => 1000]);

        $this->assertSame(1000, $config->stampedeWakeTokens);
    }

    public function test_accepts_zero_as_the_automatic_overlay_disable_value(): void
    {
        $config = CacheConfig::fromArray(['auto_overlay_max_rows' => 0]);

        $this->assertSame(0, $config->maxAutoOverlayRows);
    }

    public function test_expands_grouped_primary_key_overrides(): void
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

    public function test_validates_grouped_primary_key_overrides(): void
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

    public function test_rejects_ungrouped_primary_key_overrides(): void
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
