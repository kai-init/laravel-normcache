<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\RedisScripts;
use NormCache\Tests\TestCase;
use ReflectionProperty;

class RedisScriptsTest extends TestCase
{
    private static array $knownScripts = [
        'fetch_version_with_cooldown',
        'fetch_multi_versioned_query',
        'fetch_versioned_cache',
        'fetch_versioned_pivot',
        'fetch_versioned_query',
        'fetch_versioned_result',
        'release_building',
        'set_many_tracked_if_version',
        'store_if_versions_match_and_release',
    ];

    protected function setUp(): void
    {
        parent::setUp();

        // Clear the static cache before each test to ensure fresh loads
        $cache = new ReflectionProperty(RedisScripts::class, 'cache');
        $cache->setAccessible(true);
        $cache->setValue(null, []);
    }

    public function test_it_loads_scripts_from_filesystem(): void
    {
        foreach (self::$knownScripts as $name) {
            $script = RedisScripts::get($name);
            $this->assertIsString($script);
            $this->assertNotEmpty($script);
        }
    }

    public function test_it_caches_loaded_scripts_statically(): void
    {
        $first = RedisScripts::get('fetch_versioned_query');
        $second = RedisScripts::get('fetch_versioned_query');

        $this->assertSame($first, $second);

        $cache = new ReflectionProperty(RedisScripts::class, 'cache');
        $cache->setAccessible(true);
        $stored = $cache->getValue();

        $this->assertArrayHasKey('fetch_versioned_query', $stored);
        $this->assertSame($first, $stored['fetch_versioned_query']);

        RedisScripts::get('store_if_versions_match_and_release');
        $stored = $cache->getValue();
        $this->assertArrayHasKey('store_if_versions_match_and_release', $stored);
    }

    public function test_it_throws_exception_for_missing_scripts(): void
    {
        $this->expectException(\RuntimeException::class);
        RedisScripts::get('non_existent_script');
    }

    public function test_scripts_match_expected_content(): void
    {
        foreach (self::$knownScripts as $name) {
            $expected = file_get_contents(__DIR__ . '/../../src/Lua/' . $name . '.lua');
            $this->assertSame($expected, RedisScripts::get($name));
        }
    }

    public function test_scripts_have_stable_shas(): void
    {
        // This is a safety test to detect accidental changes to critical Lua logic
        $shas = array_map(fn($name) => sha1(RedisScripts::get($name)), self::$knownScripts);

        $this->assertCount(count(self::$knownScripts), array_unique($shas));
    }
}
