<?php

namespace NormCache\Support;

final class RedisScripts
{
    private static array $cache = [];

    public static function get(string $name): string
    {
        if (isset(self::$cache[$name])) {
            return self::$cache[$name];
        }

        $path = __DIR__ . '/../Lua/' . $name . '.lua';
        $content = @file_get_contents($path);

        if ($content === false) {
            throw new \RuntimeException("NormCache: Redis script [{$name}] not found at [{$path}].");
        }

        return self::$cache[$name] = $content;
    }
}
