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
            'ttl' => 600,
            'query_ttl' => 60,
            'primary_keys' => [],
            'deployment_ids' => ['testing' => 'test'],
            'events' => true,
        ]);

        $this->assertSame('normcache-test', $config->connection);
        $this->assertSame('app:', $config->keyPrefix);
        $this->assertSame(600, $config->ttl);
        $this->assertSame(60, $config->queryTtl);
        $this->assertSame(1000, $config->maxMembershipRows);
        $this->assertSame(1_048_576, $config->maxMembershipBytes);
        $this->assertSame(16_777_216, $config->maxCanonicalBytes);
        $this->assertSame(4_194_304, $config->maxResultBytes);
        $this->assertSame(1000, $config->maxPreciseInvalidationPks);
        $this->assertSame(10, $config->publicationGuardMarginSeconds);
        $this->assertTrue($config->dispatchEvents);
        $this->assertFalse(property_exists($config, 'cooldown'));
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
            ['max_membership_rows', 1001],
            ['max_membership_bytes', 1_048_577],
            ['max_canonical_bytes', 16_777_217],
            ['max_result_bytes', 4_194_305],
            ['max_precise_invalidation_pks', 1001],
            ['building_lock_ttl', 0],
            ['publication_guard_margin_seconds', 9],
            ['ttl', 0],
            ['query_ttl', 0],
        ];
    }

    public function test_it_validates_structured_primary_key_overrides(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('type must be integer or string');

        CacheConfig::fromArray([
            'primary_keys' => [[
                'connection' => 'tenant',
                'database' => 'app',
                'table' => 'events',
                'column' => 'event_id',
                'type' => 'uuid',
            ]],
        ]);
    }
}
