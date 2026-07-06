<?php

namespace NormCache\Values;

/** Cache dependencies contributed by one classified relation (has/withAggregate). */
final readonly class RelationDependency
{
    /**
     * @param  class-string  $relatedClass
     * @param  class-string|null  $throughClass
     * @param  list<class-string>  $constraintModels
     * @param  list<string>  $constraintTables
     */
    public function __construct(
        public string $relatedClass,
        public ?string $throughClass = null,
        public ?string $tableKey = null,
        public array $constraintModels = [],
        public array $constraintTables = [],
    ) {}

    /** @return list<class-string> */
    public function modelDependencies(): array
    {
        return [
            $this->relatedClass,
            ...($this->throughClass !== null ? [$this->throughClass] : []),
            ...$this->constraintModels,
        ];
    }

    /** @return list<string> */
    public function tableDependencies(): array
    {
        return [
            ...($this->tableKey !== null ? [$this->tableKey] : []),
            ...$this->constraintTables,
        ];
    }
}
