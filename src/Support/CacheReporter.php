<?php

namespace NormCache\Support;

use Closure;
use Illuminate\Support\Facades\Log;
use NormCache\Debug\NormCacheCollector;
use NormCache\Enums\CacheKind;
use NormCache\Enums\CacheStatus;
use NormCache\Enums\ResultKind;
use NormCache\Events\CacheInvalidated;
use NormCache\Events\CacheMetricRecorded;
use NormCache\Events\ModelCacheHit;
use NormCache\Events\ModelCacheMiss;
use NormCache\Events\QueryBypassed;
use NormCache\Events\QueryCacheHit;
use NormCache\Events\QueryCacheMiss;
use NormCache\Values\CacheSpace;

final class CacheReporter
{
    private static ?Closure $eventsEnabledResolver = null;

    public static function configureEvents(Closure $resolver): void
    {
        self::$eventsEnabledResolver = $resolver;
    }

    public static function beginMeasure(): ?float
    {
        return NormCacheCollector::beginMeasure();
    }

    public static function active(): bool
    {
        return NormCacheCollector::active() || self::eventsEnabled();
    }

    public static function cacheMeta(
        CacheKind $kind,
        CacheStatus $status,
        ?ResultKind $resultKind = null,
        ?CacheSpace $space = null,
    ): array {
        return array_filter([
            'cache_kind' => $kind->value,
            'cache_status' => $status->value,
            'result_kind' => $resultKind?->value,
            'cache_space' => $space?->name,
        ], static fn(mixed $value): bool => $value !== null);
    }

    public static function queryHit(string $modelClass, string $key, ?float $startTime, array $meta = [], string $type = 'query hit'): void
    {
        if (!self::active()) {
            return;
        }

        if (self::eventsEnabled()) {
            event(new QueryCacheHit($modelClass, $key, $meta));
        }

        NormCacheCollector::recordQuery($type, $modelClass, $key, $startTime, $meta);
    }

    public static function queryMiss(string $modelClass, string $key, ?float $startTime, array $meta = [], string $type = 'query miss'): void
    {
        if (!self::active()) {
            return;
        }

        if (self::eventsEnabled()) {
            event(new QueryCacheMiss($modelClass, $key, $meta));
        }

        NormCacheCollector::recordQuery($type, $modelClass, $key, $startTime, $meta);
    }

    public static function modelHit(string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        if (!self::active()) {
            return;
        }

        self::modelHitActive($modelClass, $ids, $startTime, $meta);
    }

    public static function modelHitActive(string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        if (self::eventsEnabled() && $ids !== []) {
            event(new ModelCacheHit($modelClass, $ids, $meta));
        }

        NormCacheCollector::recordModel('model hit', $modelClass, $ids, $startTime, $meta);
    }

    public static function modelMissActive(string $modelClass, array $ids, ?float $startTime, array $meta = []): void
    {
        if (self::eventsEnabled() && $ids !== []) {
            event(new ModelCacheMiss($modelClass, $ids, $meta));
        }

        NormCacheCollector::recordModel('model miss', $modelClass, $ids, $startTime, $meta);
    }

    public static function metric(
        string $metric,
        int|float $value,
        CacheKind $kind,
        CacheStatus $status,
        string $modelClass,
        ?ResultKind $resultKind = null,
        ?CacheSpace $space = null,
        array $meta = [],
    ): void {
        if (!self::active()) {
            return;
        }

        $fields = [
            ...self::cacheMeta($kind, $status, $resultKind, $space),
            ...$meta,
        ];

        if (self::eventsEnabled()) {
            event(new CacheMetricRecorded(
                $metric,
                $value,
                $kind,
                $status,
                $modelClass,
                $resultKind,
                $space?->name,
                $meta,
            ));
        }

        NormCacheCollector::recordMetric($metric, $value, $modelClass, $fields);
    }

    public static function invalidation(
        string $dependencyType,
        string $target,
        int $count,
        array $spaces = [],
    ): void {
        if (!self::active()) {
            return;
        }

        if (self::eventsEnabled()) {
            event(new CacheInvalidated($dependencyType, $target, $count, $spaces));
        }

        NormCacheCollector::recordInvalidation($dependencyType, $target, $count, $spaces);
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

        if (self::eventsEnabled()) {
            event(new QueryBypassed($modelClass, $bypassReasons));
        }

        NormCacheCollector::recordBypass($modelClass, $bypassReasons, $startTime);
    }

    private static function eventsEnabled(): bool
    {
        return self::$eventsEnabledResolver !== null
            ? (bool) (self::$eventsEnabledResolver)()
            : (bool) config('normcache.events', false);
    }
}
