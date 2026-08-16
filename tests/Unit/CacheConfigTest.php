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
        $this->assertSame('auto', $config->serializer);
        $this->assertSame(604_800, $config->rowTtl);
        $this->assertSame(3_600, $config->queryTtl);
        $this->assertSame(1000, $config->maxAutoOverlayRows);
        $this->assertSame(5, $config->buildingLockTtl);
        $this->assertSame(200, $config->stampedeWaitMs);
        $this->assertTrue($config->enabled);
        $this->assertTrue($config->revalidation);
        $this->assertFalse($config->dispatchEvents);
        $this->assertFalse($config->debugbar);
    }

    public function test_rejects_invalid_serializer(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('serializer');

        CacheConfig::fromArray(['serializer' => 'json']);
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
            ['building_lock_ttl', 0],
            ['row_ttl', 0],
            ['query_ttl', 0],
            ['auto_overlay_max_rows', -1],
            ['stampede_wait_ms', 0],
        ];
    }

    public function test_accepts_zero_as_the_automatic_overlay_disable_value(): void
    {
        $config = CacheConfig::fromArray(['auto_overlay_max_rows' => 0]);

        $this->assertSame(0, $config->maxAutoOverlayRows);
    }
}
