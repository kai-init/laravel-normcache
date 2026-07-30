<?php

namespace NormCache\Cache;

final class QueryHashResolver
{
    /** @var \Closure(): string */
    private \Closure $resolve;

    private ?string $value = null;

    /** @param callable(): string $resolve */
    public function __construct(callable $resolve)
    {
        $this->resolve = \Closure::fromCallable($resolve);
    }

    public function value(): string
    {
        return $this->value ??= ($this->resolve)();
    }
}
