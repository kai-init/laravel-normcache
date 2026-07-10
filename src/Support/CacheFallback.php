<?php

namespace NormCache\Support;

use NormCache\Values\CacheConfig;
use Throwable;

final class CacheFallback
{
    public static function rescue(CacheConfig $config, callable $operation, callable $fallback): mixed
    {
        return rescue(
            $operation,
            function (Throwable $e) use ($config, $fallback): mixed {
                self::fallback($config, $e);

                return $fallback();
            },
            report: false,
        );
    }

    public static function attempt(CacheConfig $config, callable $operation): bool
    {
        return rescue(
            function () use ($operation): bool {
                $operation();

                return true;
            },
            function (Throwable $e) use ($config): bool {
                self::fallback($config, $e);

                return false;
            },
            report: false,
        );
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
