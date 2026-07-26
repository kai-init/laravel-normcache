<?php

namespace NormCache\Traits;

use Closure;
use Illuminate\Database\Eloquent\Model;
use NormCache\Database\CachingQueryBuilder;

/**
 * @mixin Model
 */
trait Cacheable
{
    private ?Model $normCachePrototype = null;

    private static ?Closure $normCacheHydrate = null;

    public function newFromBuilder($attributes = [], $connection = null)
    {
        self::$normCacheHydrate ??= Closure::bind(
            static function (Model $model, array $attributes): void {
                $model->attributes = $attributes;
                $model->original = $attributes;
                $model->classCastCache = [];
                $model->attributeCastCache = [];
            },
            null,
            Model::class,
        );

        $connectionName = $connection ?: $this->getConnectionName();

        if (
            $this->normCachePrototype === null
            || $this->normCachePrototype->getConnectionName() !== $connectionName
            || $this->normCachePrototype->getTable() !== $this->getTable()
        ) {
            $this->normCachePrototype = $this->newInstance([], true);
            $this->normCachePrototype->setConnection($connectionName);
        }

        $model = clone $this->normCachePrototype;
        (self::$normCacheHydrate)($model, (array) $attributes);

        if ($this->normCacheHasRetrievedListener()) {
            $model->fireModelEvent('retrieved', false);
        }

        return $model;
    }

    /** Not memoised: observers and listeners can register at any point in a request. */
    private function normCacheHasRetrievedListener(): bool
    {
        $dispatcher = static::getEventDispatcher();

        return $dispatcher !== null
            && (isset($this->dispatchesEvents['retrieved'])
                || $dispatcher->hasListeners('eloquent.retrieved: ' . static::class));
    }

    protected function newBaseQueryBuilder()
    {
        $builder = $this->getConnection()->query();

        if (!$builder instanceof CachingQueryBuilder) {
            return $builder;
        }

        $deletedAtColumn = method_exists($this, 'getDeletedAtColumn')
            ? $this->getDeletedAtColumn()
            : null;

        return $builder->markCacheableModel(
            $this::class,
            $this->getKeyName(),
            $this->getKeyType(),
            $deletedAtColumn,
        );
    }
}
