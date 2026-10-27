<?php

namespace NormCache\Database;

use Illuminate\Database\Connection;

final class QueryStatement
{
    /** @var array{0: string, 1: list<mixed>}|null */
    private ?array $resolved = null;

    /** @var list<mixed>|null */
    private ?array $preparedBindings = null;

    /** @param \Closure(): array{0: string, 1: list<mixed>} $resolve */
    public function __construct(private \Closure $resolve) {}

    public function sql(): string
    {
        return $this->resolved()[0];
    }

    /** @return list<mixed> */
    public function bindings(): array
    {
        return $this->resolved()[1];
    }

    /** @return list<mixed> */
    public function preparedBindings(Connection $connection): array
    {
        return $this->preparedBindings ??= $connection->prepareBindings($this->bindings());
    }

    /** @return array{0: string, 1: list<mixed>} */
    private function resolved(): array
    {
        return $this->resolved ??= ($this->resolve)();
    }
}
