<?php

namespace NormCache\Support;

final class LuaScripts
{
    private static array $cache = [];

    public static function get(string $name): string
    {
        return self::$cache[$name] ??= file_get_contents(__DIR__ . '/../Lua/' . $name . '.lua');
    }
}
