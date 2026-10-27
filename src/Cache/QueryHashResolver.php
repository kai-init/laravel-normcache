<?php

namespace NormCache\Cache;

final class QueryHashResolver
{
    private ?string $value = null;

    /** @param \Closure(): string $resolve */
    public function __construct(private \Closure $resolve) {}

    public function value(): string
    {
        return $this->value ??= ($this->resolve)();
    }
}
