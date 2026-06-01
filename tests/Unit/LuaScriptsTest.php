<?php

namespace NormCache\Tests\Unit;

use NormCache\Support\LuaScripts;
use NormCache\Tests\TestCase;
use ReflectionProperty;

class LuaScriptsTest extends TestCase
{
    private static array $knownScripts = [
        'store_query_cas',
        'fetch_versioned_query',
        'fetch_versioned_cache',
        'fetch_versioned_pivot',
        'fetch_version_with_cooldown',
        'fetch_raw_by_seg',
        'set_many_tracked_if_version',
    ];

    protected function setUp(): void
    {
        parent::setUp();

        // Reset static cache so each test starts clean.
        (new ReflectionProperty(LuaScripts::class, 'cache'))->setValue(null, []);
    }

    public function test_every_script_loads_as_non_empty_string(): void
    {
        foreach (self::$knownScripts as $name) {
            $script = LuaScripts::get($name);

            $this->assertIsString($script, "Script '{$name}' did not return a string");
            $this->assertNotEmpty($script, "Script '{$name}' loaded as empty string");
        }
    }

    public function test_repeated_calls_return_same_string_instance(): void
    {
        $first = LuaScripts::get('fetch_versioned_query');
        $second = LuaScripts::get('fetch_versioned_query');

        // Identical object reference proves the static cache returned the stored value,
        // not a fresh file_get_contents call.
        $this->assertSame($first, $second);
    }

    public function test_static_cache_is_populated_after_first_load(): void
    {
        $cache = (new ReflectionProperty(LuaScripts::class, 'cache'))->getValue();
        $this->assertArrayNotHasKey('store_query_cas', $cache);

        LuaScripts::get('store_query_cas');

        $cache = (new ReflectionProperty(LuaScripts::class, 'cache'))->getValue();
        $this->assertArrayHasKey('store_query_cas', $cache);
    }

    public function test_loaded_content_matches_file_on_disk(): void
    {
        $name = 'fetch_versioned_query';
        $expected = file_get_contents(__DIR__ . '/../../src/Lua/' . $name . '.lua');

        $this->assertSame($expected, LuaScripts::get($name));
    }

    public function test_each_script_sha_is_unique(): void
    {
        $shas = array_map(fn($name) => sha1(LuaScripts::get($name)), self::$knownScripts);

        $this->assertSame($shas, array_unique($shas), 'Two scripts produce the same SHA1 — likely loading the same file twice');
    }
}
