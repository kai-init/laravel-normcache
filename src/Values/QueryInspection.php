<?php

namespace NormCache\Values;

final readonly class QueryInspection
{
    public const RAW_ORDER = 1 << 0;

    public const RAW_WHERE = 1 << 1;

    public const SUBQUERY_WHERE = 1 << 2;

    public const NON_CANONICAL_FROM = 1 << 3;

    public const JOIN = 1 << 4;

    public const GROUP = 1 << 5;

    public const HAVING = 1 << 6;

    public const UNION = 1 << 7;

    public const AGGREGATE = 1 << 8;

    public const DISTINCT = 1 << 9;

    public const LOCK = 1 << 10;

    public const CALCULATED_COLUMNS = 1 << 11;

    public const EXISTS_WHERE = 1 << 12;

    private const DEPENDENCY_BYPASS = self::RAW_ORDER | self::RAW_WHERE;

    private const NORMALIZATION_BYPASS = self::NON_CANONICAL_FROM
        | self::JOIN
        | self::GROUP
        | self::HAVING
        | self::UNION
        | self::AGGREGATE
        | self::DISTINCT
        | self::CALCULATED_COLUMNS;

    public DependencySet $dependencies;

    public function __construct(
        public int $flags = 0,
        public ?array $primaryKeys = null,
        ?DependencySet $dependencies = null,
        public array $contextReasons = [],
    ) {
        $this->dependencies = $dependencies ?? DependencySet::empty();
    }

    public function has(int $flags): bool
    {
        return ($this->flags & $flags) !== 0;
    }

    public function hasDependencyBypass(): bool
    {
        return $this->has(self::DEPENDENCY_BYPASS);
    }

    public function normalizationFlags(): int
    {
        return $this->flags & self::NORMALIZATION_BYPASS;
    }

    public function hasSafetyBypass(): bool
    {
        return $this->has(self::LOCK) || isset($this->contextReasons['safety']);
    }
}
