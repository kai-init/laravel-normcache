<?php

namespace NormCache\Tests\Unit\Support;

use NormCache\Support\RedisScripts;
use NormCache\Tests\UnitTestCase;

class RedisScriptsTest extends UnitTestCase
{
    public function test_missing_script_throws(): void
    {
        $this->expectException(\RuntimeException::class);
        RedisScripts::get('non_existent_script');
    }

    /** Every Lua script on disk must have at least one RedisScripts::get('name') call site in src/. */
    public function test_all_lua_scripts_are_referenced_by_the_source(): void
    {
        $srcDir = __DIR__ . '/../../../src';
        $luaDir = $srcDir . '/Lua';

        $scriptNames = array_map(
            fn(string $path) => basename($path, '.lua'),
            glob($luaDir . '/*.lua')
        );

        $phpSource = '';
        $iterator = new \RecursiveIteratorIterator(new \RecursiveDirectoryIterator($srcDir));
        foreach ($iterator as $file) {
            if ($file->getExtension() === 'php') {
                $phpSource .= file_get_contents($file->getPathname());
            }
        }

        preg_match_all('/RedisScripts::get\([\'"]([^\'"]+)[\'"]\)/', $phpSource, $matches);
        $usedNames = array_unique($matches[1]);

        $unused = array_diff($scriptNames, $usedNames);

        $this->assertEmpty($unused, 'Unused Lua scripts found: ' . implode(', ', $unused));
    }
}
