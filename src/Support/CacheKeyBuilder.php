<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;

class CacheKeyBuilder
{
    public const K_VER = 'ver';

    public const K_SCHEDULED = 'scheduled';

    public const K_QUERY = 'query';

    public const K_MODEL = 'model';

    public const K_BUILDING = 'building';

    public const K_MEMBERS = 'members:model';

    public const K_AGG = 'agg';

    public const K_COUNT = 'count';

    public const K_SCALAR = 'scalar';

    public const K_PIVOT = 'pivot';

    public const K_THROUGH = 'through';

    public const K_WAKE = 'wake';

    public const K_RAW = 'raw';

    private static array $classKeyCache = [];

    private static array $prototypes = [];

    public function classKey(string $class): string
    {
        return self::$classKeyCache[$class] ??= $this->resolveClassKey($class);
    }

    public function verKey(string $classKey): string
    {
        return self::K_VER . ':{' . $classKey . '}:';
    }

    public function scheduledKey(string $classKey): string
    {
        return self::K_SCHEDULED . ':{' . $classKey . '}:';
    }

    public function modelPrefix(string $classKey): string
    {
        return self::K_MODEL . ':{' . $classKey . '}:';
    }

    public function queryPrefix(string $classKey, ?string $tag = null): string
    {
        $base = self::K_QUERY . ':{' . $classKey . '}:';

        return $tag !== null ? $base . $tag . ':v' : $base . 'v';
    }

    public function rawPrefix(string $classKey): string
    {
        return self::K_RAW . ':{' . $classKey . '}:';
    }

    public function buildingPrefix(string $classKey): string
    {
        return self::K_BUILDING . ':{' . $classKey . '}:';
    }

    public function wakePrefix(string $classKey): string
    {
        return self::K_WAKE . ':{' . $classKey . '}:';
    }

    public function membersKey(string $classKey): string
    {
        return self::K_MEMBERS . ':{' . $classKey . '}';
    }

    public function tagSegment(?string $tag): string
    {
        return $tag !== null ? $tag . ':' : '';
    }

    public function modelKey(string $modelClass, string $id): string
    {
        return $this->modelPrefix($this->classKey($modelClass)) . $id;
    }

    public function depVersionKeys(string $classKey, array $depClasses): array
    {
        $all = array_merge([$classKey], array_map($this->classKey(...), $this->sortClassesByKey($depClasses)));

        return array_map(fn($key) => $this->verKey($key), $all);
    }

    public function buildingToWakeKey(string $buildingKey): string
    {
        $classKeyEnd = strpos($buildingKey, '}:') + 2;

        return self::K_WAKE
            . substr($buildingKey, strlen(self::K_BUILDING), $classKeyEnd - strlen(self::K_BUILDING))
            . substr(strrchr($buildingKey, ':'), 1);
    }

    public static function prototypeFor(string $class): Model
    {
        return self::$prototypes[$class] ??= new $class;
    }

    private function resolveClassKey(string $class): string
    {
        $model = self::prototypeFor($class);
        $connection = $model->getConnectionName() ?? DB::getDefaultConnection();

        return "{$connection}:{$model->getTable()}";
    }

    private function sortClassesByKey(array $classes): array
    {
        usort($classes, fn($a, $b) => strcmp($this->classKey($a), $this->classKey($b)));

        return $classes;
    }
}
