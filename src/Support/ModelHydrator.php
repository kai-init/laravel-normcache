<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Model;

final class ModelHydrator
{
    private static array $prototypes = [];

    private static array $closures = [];

    private static array $deletedAtColumns = [];

    public static function hydrate(
        array $ids,
        string $modelClass,
        array $raw,
        ?array $projection,
        bool $fireRetrieved,
    ): array {
        $prototype = self::prototype($modelClass);

        $closure = self::$closures[$modelClass] ??= \Closure::bind(
            static function ($model, $attributes, $fire) {
                $model->attributes = $attributes;
                $model->original = $attributes;
                $model->exists = true;
                $model->classCastCache = [];
                $model->attributeCastCache = [];

                if ($fire) {
                    $model->fireModelEvent('retrieved', false);
                }
            },
            null,
            $modelClass
        );

        $hits = [];
        $missed = [];

        foreach ($ids as $i => $id) {
            $attrs = $raw[$i];

            if ($attrs === null || $attrs === false || !is_array($attrs)) {
                $missed[] = $id;

                continue;
            }

            if ($projection !== null) {
                $attrs = QueryInspector::projectAttributes($attrs, $projection);
            }

            $instance = clone $prototype;
            $closure($instance, $attrs, $fireRetrieved);
            $hits[$id] = $instance;
        }

        return ['hits' => $hits, 'missed' => $missed];
    }

    public static function prototype(string $modelClass): Model
    {
        return self::$prototypes[$modelClass] ??= new $modelClass;
    }

    public static function deletedAtColumn(string $modelClass): ?string
    {
        return self::$deletedAtColumns[$modelClass] ??= method_exists(self::prototype($modelClass), 'getDeletedAtColumn')
            ? self::prototype($modelClass)->getDeletedAtColumn()
            : null;
    }
}
