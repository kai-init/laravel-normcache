<?php

namespace NormCache\Traits;

use Closure;
use Illuminate\Database\Eloquent\Model;
use NormCache\Database\QueryBuilder;

/**
 * @mixin Model
 */
trait Cacheable
{
    private ?Model $cachePrototype = null;

    private ?bool $cacheFastHydration = null;

    private static ?Closure $cacheHydrate = null;

    /** @var array<class-string, bool> */
    private static array $usesBaseConstruction = [];

    public function newFromBuilder($attributes = [], $connection = null)
    {
        if (!$this->usesFastHydration()) {
            return parent::newFromBuilder($attributes, $connection);
        }

        self::$cacheHydrate ??= Closure::bind(
            static function (Model $model, array $attributes): void {
                $model->attributes = $attributes;
                $model->original = $attributes;
                $model->classCastCache = [];
                $model->attributeCastCache = [];
            },
            null,
            Model::class,
        );

        $connectionName = $connection ?? $this->getConnectionName();

        if (
            $this->cachePrototype === null
            || $this->cachePrototype->getConnectionName() !== $connectionName
            || $this->cachePrototype->getTable() !== $this->getTable()
        ) {
            $this->cachePrototype = $this->newInstance([], true);
            $this->cachePrototype->setConnection($connectionName);
        }

        $model = clone $this->cachePrototype;
        (self::$cacheHydrate)($model, (array) $attributes);

        if ($this->hasRetrievedListener()) {
            $model->fireModelEvent('retrieved', false);
        }

        return $model;
    }

    private function usesFastHydration(): bool
    {
        if ($this->cacheFastHydration !== null) {
            return $this->cacheFastHydration;
        }

        if (!(self::$usesBaseConstruction[static::class] ??= $this->usesBaseConstruction())) {
            return $this->cacheFastHydration = false;
        }

        foreach (get_object_vars($this) as $property => $value) {
            if ($property !== 'cachePrototype' && is_object($value)) {
                return $this->cacheFastHydration = false;
            }
        }

        return $this->cacheFastHydration = true;
    }

    private function usesBaseConstruction(): bool
    {
        return (new \ReflectionMethod($this, '__construct'))->getDeclaringClass()->getName() === Model::class
            && (new \ReflectionMethod($this, 'newInstance'))->getDeclaringClass()->getName() === Model::class;
    }

    private function hasRetrievedListener(): bool
    {
        $dispatcher = $this->getEventDispatcher();

        return $dispatcher !== null
            && (isset($this->dispatchesEvents['retrieved'])
                || $dispatcher->hasListeners('eloquent.retrieved: ' . static::class));
    }

    protected function newBaseQueryBuilder()
    {
        $builder = $this->getConnection()->query();

        if (!$builder instanceof QueryBuilder) {
            return $builder;
        }

        $deletedAtColumn = method_exists($this, 'getDeletedAtColumn')
            ? $this->getDeletedAtColumn()
            : null;

        return $builder->enableCachingForModel(
            $this::class,
            $this->getKeyName(),
            $this->getKeyType(),
            $deletedAtColumn,
        );
    }
}
