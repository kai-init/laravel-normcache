<?php

namespace NormCache\Spaces;

use NormCache\Values\CacheSpace;
use NormCache\Values\SpaceValidationResult;

// Resolves the cache spaces a model/table belongs to. Membership is declared on the
// model (Cacheable::normCacheSpaces()); no declaration = the default space. Space name
// maps to a hash tag by convention: 'default' -> "nc", "<name>" -> "nc:<name>".
final class CacheSpaceRegistry
{
    public const DEFAULT_SPACE = 'default';

    public const DEFAULT_HASH_TAG = 'nc';

    /** @var array<string, CacheSpace> name => space (memoized) */
    private array $spaces = [];

    /** @var array<class-string, list<CacheSpace>> model class => spaces (memoized, validated) */
    private array $modelSpaces = [];

    /**
     * @param  array<string, array{hash_tag?: string}>  $placement  per-space hash-tag overrides
     */
    public function __construct(
        private readonly int $maxPerModel = 16,
        private readonly array $placement = [],
    ) {}

    public function defaultSpace(): CacheSpace
    {
        return $this->space(self::DEFAULT_SPACE);
    }

    public function space(string $name): CacheSpace
    {
        return $this->spaces[$name] ??= new CacheSpace($name, $this->hashTagFor($name));
    }

    /** @return list<CacheSpace> */
    public function materializedSpaces(): array
    {
        $spaces = $this->spaces;
        $spaces[self::DEFAULT_SPACE] ??= $this->defaultSpace();

        return array_values($spaces);
    }

    /** @return list<CacheSpace> */
    public function spacesForModel(string $modelClass): array
    {
        return $this->modelSpaces[$modelClass] ??= $this->resolveModelSpaces($modelClass);
    }

    // Tables have no model to declare membership, so they belong to the default space.
    /** @return list<CacheSpace> */
    public function spacesForTable(string $table): array
    {
        return [$this->defaultSpace()];
    }

    public function modelAllowedInSpace(string $modelClass, CacheSpace|string $space): bool
    {
        return $this->isAllowed($this->spacesForModel($modelClass), $space);
    }

    public function tableAllowedInSpace(string $table, CacheSpace|string $space): bool
    {
        return $this->isAllowed($this->spacesForTable($table), $space);
    }

    public function dependenciesAreOnlyModel(string $modelClass, array $models, array $tables): bool
    {
        return $models === [$modelClass] && $tables === [];
    }

    /**
     * Validate a cached operation's dependencies against the active space.
     *
     * @param  list<class-string>  $models
     * @param  list<string>  $tables
     */
    public function validateDependencies(
        CacheSpace $space,
        array $models,
        array $tables,
        bool $includeDependenciesBySpace = false,
    ): SpaceValidationResult {
        $invalidModels = [];
        $invalidTables = [];
        $dependencySpaces = [];

        foreach ($models as $modelClass) {
            $spaces = $this->spacesForModel($modelClass);

            if ($includeDependenciesBySpace) {
                $dependencySpaces[$modelClass] = $spaces;
            }

            if (!$this->isAllowed($spaces, $space)) {
                $invalidModels[] = $modelClass;
            }
        }

        foreach ($tables as $table) {
            $spaces = $this->spacesForTable($table);

            if ($includeDependenciesBySpace) {
                $dependencySpaces[$table] = $spaces;
            }

            if (!$this->isAllowed($spaces, $space)) {
                $invalidTables[] = $table;
            }
        }

        $ok = $invalidModels === [] && $invalidTables === [];
        $dependenciesBySpace = $includeDependenciesBySpace
            ? $this->dependencySpaceNames($dependencySpaces)
            : ($ok ? [] : $this->dependencySpaceNamesFor($models, $tables));

        return new SpaceValidationResult(
            isValid: $ok,
            space: $space,
            invalidModels: $invalidModels,
            invalidTables: $invalidTables,
            dependenciesBySpace: $dependenciesBySpace,
        );
    }

    /** @return list<CacheSpace> */
    private function resolveModelSpaces(string $modelClass): array
    {
        $names = method_exists($modelClass, 'normCacheSpaces') ? $modelClass::normCacheSpaces() : [];

        if ($names === []) {
            return [$this->defaultSpace()];
        }

        if (count($names) > $this->maxPerModel) {
            throw new \InvalidArgumentException(
                "NormCache model [{$modelClass}] declares " . count($names) . " spaces, exceeding max_per_model ({$this->maxPerModel})."
            );
        }

        return array_values(array_map(fn($name) => $this->space($name), $names));
    }

    private function hashTagFor(string $name): string
    {
        if ($name === '' || preg_match('/[:{}\s]/', $name)) {
            throw new \InvalidArgumentException(
                "Invalid cache space name [{$name}]: must be non-empty and contain no ':', '{', '}', or whitespace."
            );
        }

        // Config override for pinning a space to a specific slot/node; else by convention.
        $override = $this->placement[$name]['hash_tag'] ?? null;

        if ($override !== null) {
            if ($override === '' || preg_match('/[{}]/', $override)) {
                throw new \InvalidArgumentException(
                    "Invalid hash_tag override [{$override}] for space [{$name}]: must be non-empty and contain no '{' or '}'."
                );
            }

            return $override;
        }

        return $name === self::DEFAULT_SPACE
            ? self::DEFAULT_HASH_TAG
            : self::DEFAULT_HASH_TAG . ':' . $name;
    }

    /** @param  list<CacheSpace>  $allowed */
    private function isAllowed(array $allowed, CacheSpace|string $space): bool
    {
        $name = $space instanceof CacheSpace ? $space->name : $space;

        foreach ($allowed as $candidate) {
            if ($candidate->name === $name) {
                return true;
            }
        }

        return false;
    }

    /**
     * @param  list<CacheSpace>  $spaces
     * @return list<string>
     */
    private function spaceNames(array $spaces): array
    {
        return array_values(array_map(fn(CacheSpace $s) => $s->name, $spaces));
    }

    /**
     * @param  array<string, list<CacheSpace>>  $dependencies
     * @return array<string, list<string>>
     */
    private function dependencySpaceNames(array $dependencies): array
    {
        $names = [];

        foreach ($dependencies as $dependency => $spaces) {
            $names[$dependency] = $this->spaceNames($spaces);
        }

        return $names;
    }

    /**
     * @param  list<class-string>  $models
     * @param  list<string>  $tables
     * @return array<string, list<string>>
     */
    private function dependencySpaceNamesFor(array $models, array $tables): array
    {
        $names = [];

        foreach ($models as $modelClass) {
            $names[$modelClass] = $this->spaceNames($this->spacesForModel($modelClass));
        }

        foreach ($tables as $table) {
            $names[$table] = $this->spaceNames($this->spacesForTable($table));
        }

        return $names;
    }
}
