<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Model;

/** Memoized closures bound into Model for direct attribute access, bypassing casts and mutators. */
final class RawAttributes
{
    private static ?\Closure $hydrateClosure = null;

    private static ?\Closure $setAttributeClosure = null;

    private static ?\Closure $getAttributeClosure = null;

    public static function hydrateClosure(): \Closure
    {
        return self::$hydrateClosure ??= \Closure::bind(
            static function (Model $instance, array $attrs, bool $fire): void {
                $instance->attributes = $attrs;
                $instance->original = $attrs;
                $instance->classCastCache = [];
                $instance->attributeCastCache = [];
                $instance->exists = true;
                if ($fire) {
                    $instance->fireModelEvent('retrieved', false);
                }
            },
            null,
            Model::class
        );
    }

    public static function setAttributeClosure(): \Closure
    {
        return self::$setAttributeClosure ??= \Closure::bind(
            static function (Model $instance, string $key, mixed $value): void {
                $instance->attributes[$key] = $value;
            },
            null,
            Model::class
        );
    }

    public static function getAttributeClosure(): \Closure
    {
        return self::$getAttributeClosure ??= \Closure::bind(
            static function (Model $instance, string $key): mixed {
                return $instance->attributes[$key] ?? null;
            },
            null,
            Model::class
        );
    }
}
