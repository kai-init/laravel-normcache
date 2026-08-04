<?php

namespace NormCache\Values;

use Illuminate\Database\Eloquent\Model;

final readonly class DependencyDeclaration
{
    public const TABLE = 'table';

    public const MODEL = 'model';

    /** @param class-string<Model>|string $value */
    private function __construct(
        public string $type,
        public string $value,
    ) {}

    public static function table(string $table): self
    {
        return new self(self::TABLE, $table);
    }

    /** @param class-string<Model> $model */
    public static function model(string $model): self
    {
        return new self(self::MODEL, $model);
    }

    public function key(): string
    {
        return $this->type . ':' . $this->value;
    }

    public function isTable(): bool
    {
        return $this->type === self::TABLE;
    }
}
