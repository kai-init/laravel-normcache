<?php

namespace NormCache\Spaces;

use NormCache\Support\RedisStore;
use NormCache\Values\CacheSpace;
use NormCache\Values\SpaceValidationResult;

// Model-declared cache-space registry. Non-default spaces are persisted for
// Redis Cluster flush discovery; undeclared models use the default space.
final class CacheSpaceRegistry
{
    public const DEFAULT_SPACE = 'default';

    public const DEFAULT_HASH_TAG = 'nc';

    /** @var array<string, CacheSpace> name => space (memoized) */
    private array $spaces = [];

    /** @var array<class-string, list<CacheSpace>> model class => spaces (memoized, validated) */
    private array $modelSpaces = [];

    /** @var array<string, list<CacheSpace>> table key => spaces (memoized, validated) */
    private array $tableSpaces = [];

    /** @var list<string>|null */
    private ?array $metadataSpaceNames = null;

    /**
     * @param  array<string, array{hash_tag?: string}>  $placement  per-space hash-tag overrides
     */
    public function __construct(
        private readonly int $maxPerModel = 16,
        private readonly array $placement = [],
        private readonly ?RedisStore $metadataStore = null,
        private readonly string $metadataKeyPrefix = '',
    ) {}

    public function defaultSpace(): CacheSpace
    {
        return $this->space(self::DEFAULT_SPACE);
    }

    public function space(string $name): CacheSpace
    {
        return $this->materializeSpace($name);
    }

    /** @return list<CacheSpace> */
    public function knownSpaces(): array
    {
        $names = array_values(array_unique([
            self::DEFAULT_SPACE,
            ...array_keys($this->placement),
            ...$this->metadataSpaces(),
            ...array_keys($this->spaces),
        ]));

        return array_map(fn(string $name) => $this->materializeSpace($name, remember: false), $names);
    }

    /** @return list<CacheSpace> */
    public function spacesForModel(string $modelClass): array
    {
        return $this->modelSpaces[$modelClass] ??= $this->resolveModelSpaces($modelClass);
    }

    /** @return list<CacheSpace> */
    public function spacesForTable(string $table): array
    {
        return $this->tableSpaces[$table] ??= $this->resolveTableSpaces($table);
    }

    public function modelAllowedInSpace(string $modelClass, CacheSpace|string $space): bool
    {
        return $this->isAllowed($this->spacesForModel($modelClass), $space);
    }

    public function dependenciesAreOnlyModel(string $modelClass, array $models, array $tables): bool
    {
        return $models === [$modelClass] && $tables === [];
    }

    /**
     * Validate operation dependencies against the active space.
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
        $dependencySpaces = [];

        foreach ($models as $modelClass) {
            $spaces = $this->spacesForModel($modelClass);
            $dependencySpaces[$modelClass] = $spaces;

            if (!$this->isAllowed($spaces, $space)) {
                $invalidModels[] = $modelClass;
            }
        }

        foreach ($tables as $table) {
            $spaces = $this->spacesForTable($table);
            $validatedSpaces = $this->isAllowed($spaces, $space)
                ? $spaces
                : [...$spaces, $space];

            $dependencySpaces[$table] = $validatedSpaces;
        }

        $ok = $invalidModels === [];
        $dependenciesBySpace = $ok && !$includeDependenciesBySpace
            ? []
            : $this->dependencySpaceNames($dependencySpaces);

        return new SpaceValidationResult(
            isValid: $ok,
            space: $space,
            invalidModels: $invalidModels,
            invalidTables: [],
            dependenciesBySpace: $dependenciesBySpace,
        );
    }

    /** @return list<CacheSpace> */
    private function resolveModelSpaces(string $modelClass): array
    {
        $names = array_values(array_unique(
            method_exists($modelClass, 'normCacheSpaces') ? $modelClass::normCacheSpaces() : []
        ));

        if ($names === []) {
            return [$this->defaultSpace()];
        }

        if (count($names) > $this->maxPerModel) {
            throw new \InvalidArgumentException(
                "NormCache model [{$modelClass}] declares " . count($names) . " spaces, exceeding max_per_model ({$this->maxPerModel})."
            );
        }

        return array_map(fn($name) => $this->space($name), $names);
    }

    /** @return list<CacheSpace> */
    private function resolveTableSpaces(string $table): array
    {
        $names = array_values(array_unique([
            self::DEFAULT_SPACE,
            ...$this->metadataTableSpaces($table),
        ]));

        return array_map(fn(string $name) => $this->materializeSpace($name, remember: false), $names);
    }

    private function materializeSpace(string $name, bool $remember = true): CacheSpace
    {
        if (!isset($this->spaces[$name])) {
            $this->spaces[$name] = new CacheSpace($name, $this->hashTagFor($name));

            if ($remember) {
                $this->rememberSpace($name);
            }
        }

        return $this->spaces[$name];
    }

    private function hashTagFor(string $name): string
    {
        if (!$this->validSpaceName($name)) {
            throw new \InvalidArgumentException(
                "Invalid cache space name [{$name}]: must be non-empty and contain no ':', '{', '}', or whitespace."
            );
        }

        // Placement override; otherwise use the standard hash-tag convention.
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

    private function validSpaceName(string $name): bool
    {
        return $name !== '' && !preg_match('/[:{}\s]/', $name);
    }

    /** @return list<string> */
    private function metadataSpaces(): array
    {
        if ($this->metadataStore === null) {
            return [];
        }

        if ($this->metadataSpaceNames !== null) {
            return $this->metadataSpaceNames;
        }

        try {
            return $this->metadataSpaceNames = array_values(array_filter(
                $this->metadataStore->setMembers($this->metadataSpacesKey()),
                fn(string $name) => $this->validSpaceName($name),
            ));
        } catch (\Throwable) {
            return $this->metadataSpaceNames = [];
        }
    }

    /** @return list<string> */
    private function metadataTableSpaces(string $table): array
    {
        if ($this->metadataStore === null) {
            return [];
        }

        try {
            return array_values(array_filter(
                $this->metadataStore->setMembers($this->metadataTableSpacesKey($table)),
                fn(string $name) => $this->validSpaceName($name),
            ));
        } catch (\Throwable) {
            return [];
        }
    }

    private function rememberSpace(string $name): void
    {
        if ($this->metadataStore === null || $name === self::DEFAULT_SPACE) {
            return;
        }

        try {
            $this->metadataStore->addToSet($this->metadataSpacesKey(), [$name]);
        } catch (\Throwable $e) {
            report($e);
        }
    }

    public function registerTableDependencies(CacheSpace $space, array $tables): void
    {
        foreach ($tables as $table) {
            $this->rememberTableSpace($table, $space);
        }
    }

    /** @return list<CacheSpace> */
    private function rememberTableSpace(string $table, CacheSpace $space): array
    {
        $spaces = $this->spacesForTable($table);

        if (!$this->isAllowed($spaces, $space)) {
            $spaces[] = $space;
            $this->tableSpaces[$table] = $spaces;
            $this->persistTableSpace($table, $space);
        }

        return $spaces;
    }

    private function persistTableSpace(string $table, CacheSpace $space): void
    {
        if ($this->metadataStore === null || $space->name === self::DEFAULT_SPACE) {
            return;
        }

        try {
            $this->metadataStore->addToSet($this->metadataTableSpacesKey($table), [$space->name]);
        } catch (\Throwable $e) {
            report($e);
        }
    }

    private function metadataSpacesKey(): string
    {
        return '{nc:meta}:' . $this->metadataKeyPrefix . 'spaces';
    }

    private function metadataTableSpacesKey(string $table): string
    {
        return '{nc:meta}:' . $this->metadataKeyPrefix . 'table-spaces:' . sha1($table);
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
        return array_map(fn(CacheSpace $s) => $s->name, $spaces);
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
}
