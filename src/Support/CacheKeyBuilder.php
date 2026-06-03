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

    public const K_COUNT = 'count';

    public const K_SCALAR = 'scalar';

    public const K_PIVOT = 'pivot';

    public const K_THROUGH = 'through';

    public const K_WAKE = 'wake';

    public const K_RESULT = 'result';

    private static array $classKeys = [];

    private static array $prototypes = [];

    private static array $deletedAtColumns = [];

    // -------------------------------------------------------------------------
    // Prefixes
    // -------------------------------------------------------------------------

    public function modelPrefix(string $classKey): string
    {
        return self::K_MODEL . ':{' . $classKey . '}:';
    }

    public function queryPrefix(string $classKey, ?string $tag = null): string
    {
        $base = self::K_QUERY . ':{' . $classKey . '}:';

        return $tag !== null ? $base . $tag . ':v' : $base . 'v';
    }

    public function resultPrefix(string $classKey): string
    {
        return self::K_RESULT . ':{' . $classKey . '}:';
    }

    public function namespacedPrefix(string $namespace, string $classKey, ?string $tag = null): string
    {
        return "{$namespace}:{{$classKey}}:" . $this->tagSegment($tag);
    }

    public function pivotBasePrefix(string $parentKey, string $relatedKey): string
    {
        return self::K_PIVOT . ':{' . $parentKey . '}:' . $relatedKey . ':';
    }

    public function pivotPrefix(string $parentKey, string $relatedKey, string $relation, string $constraintHash, string $seg): string
    {
        return $this->pivotBasePrefix($parentKey, $relatedKey) . $relation . ':' . $constraintHash . ':' . $seg . ':';
    }

    public function throughPrefix(string $relatedKey, string $throughKey): string
    {
        return self::K_THROUGH . ':{' . $relatedKey . '}:' . $throughKey . ':';
    }

    public function buildingPrefix(string $classKey): string
    {
        return self::K_BUILDING . ':{' . $classKey . '}:';
    }

    public function wakePrefix(string $classKey): string
    {
        return self::K_WAKE . ':{' . $classKey . '}:';
    }

    // -------------------------------------------------------------------------
    // High-level resolution
    // -------------------------------------------------------------------------

    public function classKey(string $class): string
    {
        return self::$classKeys[$class] ??= $this->resolveClassKey($class);
    }

    public static function prototype(string $class): Model
    {
        return self::$prototypes[$class] ??= new $class;
    }

    public static function deletedAtColumn(string $class): ?string
    {
        return self::$deletedAtColumns[$class] ??= method_exists(self::prototype($class), 'getDeletedAtColumn')
            ? self::prototype($class)->getDeletedAtColumn()
            : null;
    }

    public function tableKey(string $connectionName, string $table): string
    {
        return "{$connectionName}:{$table}";
    }

    public function verKey(string $classKey): string
    {
        return self::K_VER . ':{' . $classKey . '}:';
    }

    public function scheduledKey(string $classKey): string
    {
        return self::K_SCHEDULED . ':{' . $classKey . '}:';
    }

    public function membersKey(string $classKey): string
    {
        return self::K_MEMBERS . ':{' . $classKey . '}';
    }

    public function wakeKey(string $classKey, string $lockSuffix): string
    {
        return self::K_WAKE . ':{' . $classKey . '}:' . $lockSuffix;
    }

    // -------------------------------------------------------------------------
    // Specific Keys
    // -------------------------------------------------------------------------

    public function queryKey(string $classKey, ?string $tag, int|string $version, string $hash): string
    {
        return $this->queryPrefix($classKey, $tag) . $version . ':' . $hash;
    }

    public function namespacedKey(string $namespace, string $classKey, ?string $tag, string $seg, string $hash): string
    {
        return $this->namespacedPrefix($namespace, $classKey, $tag) . $seg . ':' . $hash;
    }

    public function resultKey(string $classKey, ?string $tag, string $seg, string $hash): string
    {
        return $this->resultPrefix($classKey) . $this->tagSegment($tag) . $seg . ':' . $hash;
    }

    public function resultBuildingKey(string $classKey, string $seg, string $lockSuffix): string
    {
        return $this->buildingPrefix($classKey) . $seg . ':' . $lockSuffix;
    }

    public function throughKey(string $relatedKey, string $throughKey, string $seg, string $hash): string
    {
        return $this->throughPrefix($relatedKey, $throughKey) . $seg . ':' . $hash;
    }

    public function pivotKey(string $parentKey, string $relatedKey, string $relation, string $constraintHash, string $seg, mixed $parentId): string
    {
        return $this->pivotPrefix($parentKey, $relatedKey, $relation, $constraintHash, $seg) . $parentId;
    }

    public function modelKey(string $modelClass, string $id): string
    {
        return $this->modelPrefix($this->classKey($modelClass)) . $id;
    }

    // -------------------------------------------------------------------------
    // Versioning Helpers
    // -------------------------------------------------------------------------

    public function versionedKey(string $keyPrefix, string $seg, string $hash): string
    {
        return $keyPrefix . $seg . ':' . $hash;
    }

    public function versionSegment(array $versionKeys, array $resolvedVersions): string
    {
        return implode(':', array_map(fn($key) => 'v' . $resolvedVersions[$key], $versionKeys));
    }

    public function versionsFromSegment(string $seg): array
    {
        return array_map(fn($version) => substr($version, 1), explode(':', $seg));
    }

    public function buildingToWakeKey(string $buildingKey): string
    {
        $classKeyEnd = strpos($buildingKey, '}:') + 2;

        return self::K_WAKE
            . substr($buildingKey, strlen(self::K_BUILDING), $classKeyEnd - strlen(self::K_BUILDING))
            . substr(strrchr($buildingKey, ':'), 1);
    }

    // -------------------------------------------------------------------------
    // Dependency Resolvers
    // -------------------------------------------------------------------------

    public function depVersionKeys(string $classKey, array $depClasses, array $depTableKeys = []): array
    {
        $all = array_values(array_unique(
            array_merge([$classKey], array_map($this->classKey(...), $this->sortClassesByKey($depClasses)), $this->sortKeys($depTableKeys))
        ));

        return array_map(fn($key) => $this->verKey($key), $all);
    }

    public function depScheduledKeys(string $classKey, array $depClasses, array $depTableKeys = []): array
    {
        $all = array_values(array_unique(
            array_merge([$classKey], array_map($this->classKey(...), $this->sortClassesByKey($depClasses)), $this->sortKeys($depTableKeys))
        ));

        return array_map(fn($key) => $this->scheduledKey($key), $all);
    }

    // -------------------------------------------------------------------------
    // Suffixes / Segments
    // -------------------------------------------------------------------------

    public function tagSegment(?string $tag): string
    {
        return $tag !== null ? $tag . ':' : '';
    }

    public function resultBuildIdentityHash(?string $tag, string $hash): string
    {
        return sha1($this->tagSegment($tag) . $hash);
    }

    // -------------------------------------------------------------------------
    // Private implementation
    // -------------------------------------------------------------------------

    private function resolveClassKey(string $class): string
    {
        $model = self::prototype($class);
        $connection = $model->getConnectionName() ?? DB::getDefaultConnection();

        return "{$connection}:{$model->getTable()}";
    }

    private function sortClassesByKey(array $classes): array
    {
        usort($classes, fn($a, $b) => strcmp($this->classKey($a), $this->classKey($b)));

        return $classes;
    }

    private function sortKeys(array $keys): array
    {
        sort($keys, SORT_STRING);

        return $keys;
    }
}
