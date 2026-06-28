<?php

namespace NormCache\Support;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use NormCache\Cache\ModelHydrator;
use NormCache\Values\CacheSpace;

class CacheKeyBuilder
{
    public const K_VER = 'ver';

    public const K_SCHEDULED = 'scheduled';

    public const K_QUERY = 'query';

    public const K_MODEL = 'model';

    public const K_BUILDING = 'building';

    public const K_COUNT = 'count';

    public const K_SCALAR = 'scalar';

    public const K_PIVOT = 'pivot';

    public const K_THROUGH = 'through';

    public const K_WAKE = 'wake';

    public const K_RESULT = 'result';

    private static array $classKeys = [];

    private static array $prototypes = [];

    private static array $deletedAtColumns = [];

    private static array $singleDepPairs = [];

    public function __construct(
        private readonly string $hashTagPrefix = '{nc}:',
        private readonly string $keyPrefix = '',
    ) {}

    private function full(string $body, ?CacheSpace $space = null): string
    {
        return $this->tagPrefix($space) . $this->keyPrefix . $body;
    }

    // Hash-tag prefix for a space ("{nc:content}:"), or the configured default ("{nc}:").
    private function tagPrefix(?CacheSpace $space): string
    {
        return $space === null ? $this->hashTagPrefix : '{' . $space->hashTag . '}:';
    }

    public function prefixed(string $pattern, ?CacheSpace $space = null): string
    {
        return $this->full($pattern, $space);
    }

    public function nullKey(?CacheSpace $space = null): string
    {
        return $this->tagPrefix($space) . 'null';
    }

    // -------------------------------------------------------------------------
    // Prefixes
    // -------------------------------------------------------------------------

    public function modelPrefix(string $classKey, int|string $version, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_MODEL . ':' . $classKey . ':v' . $version . ':', $space);
    }

    public function queryPrefix(string $classKey, ?string $tag = null, ?CacheSpace $space = null): string
    {
        $base = self::K_QUERY . ':' . $classKey . ':';

        return $this->full($tag !== null ? $base . $tag . ':' : $base, $space);
    }

    public function namespacedPrefix(string $namespace, string $classKey, ?string $tag = null, ?CacheSpace $space = null): string
    {
        return $this->full("{$namespace}:{$classKey}:" . $this->tagSegment($tag), $space);
    }

    public function pivotBasePrefix(string $parentKey, string $relatedKey, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_PIVOT . ':' . $parentKey . ':' . $relatedKey . ':', $space);
    }

    public function pivotPrefix(string $parentKey, string $relatedKey, string $relation, string $constraintHash, string $seg, ?CacheSpace $space = null): string
    {
        return $this->pivotBasePrefix($parentKey, $relatedKey, $space) . $relation . ':' . $constraintHash . ':' . $seg . ':';
    }

    public function buildingPrefix(string $classKey, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_BUILDING . ':' . $classKey . ':', $space);
    }

    public function wakePrefix(string $classKey, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_WAKE . ':' . $classKey . ':', $space);
    }

    // -------------------------------------------------------------------------
    // High-level resolution
    // -------------------------------------------------------------------------

    public function classKey(string $class): string
    {
        return self::$classKeys[$class] ??= $this->resolveClassKey($class);
    }

    // Clear all static metadata caches. Call this after switching tenant connections.
    public static function reset(): void
    {
        self::$classKeys = [];
        self::$prototypes = [];
        self::$deletedAtColumns = [];
        self::$singleDepPairs = [];

        ModelHydrator::reset();
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

    public function verKey(string $classKey, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_VER . ':' . $classKey . ':', $space);
    }

    public function scheduledKey(string $classKey, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_SCHEDULED . ':' . $classKey . ':', $space);
    }

    public function wakeKey(string $classKey, string $lockSuffix, ?CacheSpace $space = null): string
    {
        return $this->full(self::K_WAKE . ':' . $classKey . ':' . $lockSuffix, $space);
    }

    // -------------------------------------------------------------------------
    // Specific Keys
    // -------------------------------------------------------------------------

    public function queryKey(string $classKey, ?string $tag, int|string $version, string $hash, ?CacheSpace $space = null): string
    {
        return $this->queryPrefix($classKey, $tag, $space) . 'v' . $version . ':' . $hash;
    }

    public function namespacedKey(string $namespace, string $classKey, ?string $tag, string $seg, string $hash, ?CacheSpace $space = null): string
    {
        return $this->namespacedPrefix($namespace, $classKey, $tag, $space) . $seg . ':' . $hash;
    }

    public function resultBuildingKey(string $classKey, string $seg, string $lockSuffix, ?CacheSpace $space = null): string
    {
        return $this->buildingPrefix($classKey, $space) . $seg . ':' . $lockSuffix;
    }

    public function pivotKey(string $parentKey, string $relatedKey, string $relation, string $constraintHash, string $seg, mixed $parentId, ?CacheSpace $space = null): string
    {
        return $this->pivotPrefix($parentKey, $relatedKey, $relation, $constraintHash, $seg, $space) . $parentId;
    }

    // -------------------------------------------------------------------------
    // Versioning Helpers
    // -------------------------------------------------------------------------

    public function versionSegment(array $versionKeys, array $resolvedVersions): string
    {
        $versions = [];

        foreach ($versionKeys as $key) {
            $versions[] = 'v' . $resolvedVersions[$key];
        }

        return implode(':', $versions);
    }

    public function versionsFromSegment(string $seg): array
    {
        $parts = explode(':', $seg);

        foreach ($parts as $i => $version) {
            $parts[$i] = substr($version, 1);
        }

        return $parts;
    }

    public function buildingToWakeKey(string $buildingKey): string
    {
        $keyword = self::K_BUILDING . ':';
        $pos = strpos($buildingKey, $keyword);
        $afterKeyword = $pos + strlen($keyword);

        $connEnd = strpos($buildingKey, ':', $afterKeyword);
        $tableEnd = strpos($buildingKey, ':', $connEnd + 1);

        $head = substr($buildingKey, 0, $tableEnd);
        $head = substr_replace($head, self::K_WAKE, $pos, strlen(self::K_BUILDING));

        $hash = substr(strrchr($buildingKey, ':'), 1);

        return $head . ':' . $hash;
    }

    // -------------------------------------------------------------------------
    // Dependency Resolvers
    // -------------------------------------------------------------------------

    /**
     * @return array{0: list<string>, 1: list<string>} [versionKeys, scheduledKeys]
     */
    public function depKeyPairs(string $classKey, array $depClasses, array $depTableKeys = [], ?CacheSpace $space = null): array
    {
        if ($depClasses === [] && $depTableKeys === []) {
            $cacheKey = ($space?->name ?? '') . '|' . $classKey;

            return self::$singleDepPairs[$cacheKey] ??= [
                [$this->verKey($classKey, $space)],
                [$this->scheduledKey($classKey, $space)],
            ];
        }

        $all = [];
        $seen = [];

        $seen[$classKey] = true;
        $all[] = $classKey;

        foreach ($this->sortClassesByKey($depClasses) as $class) {
            $key = $this->classKey($class);

            if (!isset($seen[$key])) {
                $seen[$key] = true;
                $all[] = $key;
            }
        }

        foreach ($this->sortKeys($depTableKeys) as $key) {
            if (!isset($seen[$key])) {
                $seen[$key] = true;
                $all[] = $key;
            }
        }

        $versionKeys = [];
        $scheduledKeys = [];

        foreach ($all as $key) {
            $versionKeys[] = $this->verKey($key, $space);
            $scheduledKeys[] = $this->scheduledKey($key, $space);
        }

        return [$versionKeys, $scheduledKeys];
    }

    // -------------------------------------------------------------------------
    // Suffixes / Segments
    // -------------------------------------------------------------------------

    public function tagSegment(?string $tag): string
    {
        return $tag !== null ? $tag . ':' : '';
    }

    // Tags become raw key segments, so reserved key characters must be rejected.
    public static function assertValidTag(string $tag): void
    {
        if ($tag === '' || preg_match('/[:{}\s*]/', $tag)) {
            throw new \InvalidArgumentException(
                'Cache tag must be non-empty and must not contain reserved characters (: { } * or whitespace).'
            );
        }
    }

    public function resultBuildIdentityHash(string $namespace, ?string $tag, string $hash): string
    {
        return hash('xxh3', $namespace . ':' . $this->tagSegment($tag) . $hash);
    }

    // -------------------------------------------------------------------------
    // Private implementation
    // -------------------------------------------------------------------------

    private function resolveClassKey(string $class): string
    {
        $model = self::prototype($class);
        $connection = $model->getConnectionName() ?? DB::getDefaultConnection();

        if (str_contains($connection, ':')) {
            throw new \InvalidArgumentException(
                "NormCache connection name [{$connection}] must not contain a colon; the class key is colon-delimited."
            );
        }

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
