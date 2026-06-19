<?php

namespace NormCache\Support;

use NormCache\Values\CacheConfig;
use Throwable;

final class FallbackHandler
{
    public static function rescue(CacheConfig $config, callable $operation, callable $fallback): mixed
    {
        try {
            return $operation();
        } catch (Throwable $e) {
            self::fallback($config, $e);
        }

        return $fallback();
    }

    public static function attempt(CacheConfig $config, callable $operation): bool
    {
        try {
            $operation();

            return true;
        } catch (Throwable $e) {
            self::fallback($config, $e);

            return false;
        }
    }

    public static function fallback(CacheConfig $config, Throwable $e): void
    {
        if (!$config->fallbackEnabled) {
            throw $e;
        }

        report($e);
        $config->enabled = false;
    }
}
