<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Collection;

/** Applies a model's casts/mutators to raw scalar values from cached pluck()/value() results. */
final class ScalarTransformer
{
    private static ?\Closure $transformScalarClosure = null;

    private static ?\Closure $transformScalarsClosure = null;

    private const STATELESS_CASTS = [
        'array' => true,
        'bool' => true,
        'boolean' => true,
        'collection' => true,
        'custom_datetime' => true,
        'date' => true,
        'datetime' => true,
        'decimal' => true,
        'double' => true,
        'float' => true,
        'immutable_custom_datetime' => true,
        'immutable_date' => true,
        'immutable_datetime' => true,
        'int' => true,
        'integer' => true,
        'json' => true,
        'json:unicode' => true,
        'object' => true,
        'real' => true,
        'string' => true,
        'timestamp' => true,
    ];

    public static function transformScalar(mixed $value, Model $model, string $column): mixed
    {
        $isCast = self::resolveStatelessScalarMode($model, $column);

        if ($isCast === null) {
            return $model->newFromBuilder([$column => $value])->{$column};
        }

        return self::transformScalarClosure()($model, $column, $value, $isCast);
    }

    public static function transformScalars(Collection $results, Model $model, string $column): Collection
    {
        $isCast = self::resolveStatelessScalarMode($model, $column);
        $values = $results->all();

        if ($isCast === null) {
            $template = $model->newInstance([], true);
            $hydrate = RawAttributes::hydrateClosure();

            foreach ($values as $key => $value) {
                $instance = clone $template;
                $hydrate($instance, [$column => $value], true);
                $values[$key] = $instance->{$column};
            }

            return new Collection($values);
        }

        return new Collection(
            self::transformScalarsClosure()($model, $column, $values, $isCast),
        );
    }

    private static function resolveStatelessScalarMode(Model $model, string $column): ?bool
    {
        if ($model->hasAnyGetMutator($column)) {
            return null;
        }

        $cast = $model->getCasts()[$column] ?? null;

        if ($cast === null) {
            if (!in_array($column, $model->getDates(), true)) {
                return null;
            }

            $isCast = false;
        } elseif (!is_string($cast)) {
            return null;
        } else {
            $cast = strtolower(explode(':', $cast, 2)[0]);

            if (!isset(self::STATELESS_CASTS[$cast])) {
                return null;
            }

            $isCast = true;
        }

        $dispatcher = $model::getEventDispatcher();

        if ($dispatcher !== null && $dispatcher->hasListeners('eloquent.retrieved: ' . $model::class)) {
            return null;
        }

        return $isCast;
    }

    private static function transformScalarClosure(): \Closure
    {
        return self::$transformScalarClosure ??= \Closure::bind(
            static function (Model $model, string $column, mixed $value, bool $isCast): mixed {
                if ($isCast) {
                    return $model->castAttribute($column, $value);
                }

                return $value === null ? null : $model->asDateTime($value);
            },
            null,
            Model::class
        );
    }

    private static function transformScalarsClosure(): \Closure
    {
        return self::$transformScalarsClosure ??= \Closure::bind(
            static function (Model $model, string $column, array $values, bool $isCast): array {
                if ($isCast) {
                    foreach ($values as $key => $value) {
                        $values[$key] = $model->castAttribute($column, $value);
                    }

                    return $values;
                }

                foreach ($values as $key => $value) {
                    $values[$key] = $value === null ? null : $model->asDateTime($value);
                }

                return $values;
            },
            null,
            Model::class
        );
    }
}
