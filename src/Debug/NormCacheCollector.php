<?php

namespace NormCache\Debug;

/**
 * Static dispatch facade for the debugbar collector.
 * Has no dependency on php-debugbar — safe to load when debugbar is not installed.
 * All methods are no-ops when the collector has not been registered.
 */
final class NormCacheCollector
{
    private static ?object $instance = null;

    public static function register(object $collector): void
    {
        self::$instance = $collector;
    }

    public static function active(): bool
    {
        return self::$instance !== null;
    }

    /** Returns the current timestamp only when a collector is active, avoiding microtime() overhead otherwise. */
    public static function beginMeasure(): ?float
    {
        return self::$instance !== null ? microtime(true) : null;
    }

    public static function recordQuery(string $type, string $modelClass, string $key, ?float $startTime, array $meta = []): void
    {
        self::$instance?->addQueryMeasure($type, $modelClass, $key, $startTime, $meta);
    }

    public static function recordModel(string $type, string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        self::$instance?->addModelMeasure($type, $modelClass, $ids, $startTime, $meta);
    }

    public static function recordBypass(string $modelClass, array $groupedReasons, ?float $startTime): void
    {
        self::$instance?->addBypassMeasure($modelClass, $groupedReasons, $startTime);
    }
}
