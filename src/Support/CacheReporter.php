<?php

namespace NormCache\Support;

use Illuminate\Support\Facades\Log;
use NormCache\Debug\NormCacheCollector;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Facades\NormCache;

final class CacheReporter
{
    public static function beginMeasure(): ?float
    {
        return NormCacheCollector::beginMeasure();
    }

    public static function active(): bool
    {
        return NormCacheCollector::active() || NormCache::isEventsEnabled();
    }

    public static function queryHit(string $modelClass, string $key, ?float $startTime, array $meta = [], string $type = 'query hit'): void
    {
        if (!self::active()) {
            return;
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheHit($modelClass, $key));
        }

        NormCacheCollector::recordQuery($type, $modelClass, $key, $startTime, $meta);
    }

    public static function queryMiss(string $modelClass, string $key, ?float $startTime, array $meta = [], string $type = 'query miss'): void
    {
        if (!self::active()) {
            return;
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryCacheMiss($modelClass, $key));
        }

        NormCacheCollector::recordQuery($type, $modelClass, $key, $startTime, $meta);
    }

    public static function modelHit(string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        if (!self::active()) {
            return;
        }

        if (NormCache::isEventsEnabled() && $ids !== []) {
            event(new ModelCacheHit($modelClass, $ids));
        }

        NormCacheCollector::recordModel('model hit', $modelClass, $ids, $startTime, $meta);
    }

    public static function modelMiss(string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        if (!self::active()) {
            return;
        }

        if (NormCache::isEventsEnabled() && $ids !== []) {
            event(new ModelCacheMiss($modelClass, $ids));
        }

        NormCacheCollector::recordModel('model miss', $modelClass, $ids, $startTime, $meta);
    }

    /** @param array<string, list<string>> $bypassReasons */
    public static function queryBypassed(string $modelClass, array $bypassReasons, ?float $startTime = null): void
    {
        if (config('app.debug', false) && !empty($bypassReasons['dependency'])) {
            Log::warning(sprintf(
                'NormCache Warning: Query on %s bypassed cache due to unsafe dependency inference (%s). Please provide explicit dependsOn() or dependsOnTables() to enable caching.',
                $modelClass,
                implode(', ', $bypassReasons['dependency'])
            ));
        }

        if (!self::active()) {
            return;
        }

        if (NormCache::isEventsEnabled()) {
            event(new QueryBypassed($modelClass, $bypassReasons));
        }

        NormCacheCollector::recordBypass($modelClass, $bypassReasons, $startTime);
    }
}
