<?php

namespace NormCache\Cache;

final class CacheFlowGuard
{
    private bool $enabled = true;

    /** @var callable(\Throwable): void */
    private $reporter;

    public function __construct(
        private bool $fallbackEnabled,
        ?callable $reporter = null,
    ) {
        $this->reporter = $reporter ?? static fn(\Throwable $e) => report($e);
    }

    public function isEnabled(): bool
    {
        return $this->enabled;
    }

    public function enable(): void
    {
        $this->enabled = true;
    }

    public function disable(): void
    {
        $this->enabled = false;
    }

    public function rescue(callable $operation, callable $fallback): mixed
    {
        try {
            return $operation();
        } catch (\Throwable $e) {
            $this->fallback($e);
        }

        return $fallback();
    }

    public function attempt(callable $operation): bool
    {
        try {
            $operation();

            return true;
        } catch (\Throwable $e) {
            $this->fallback($e);

            return false;
        }
    }

    public function fallback(\Throwable $e): void
    {
        if (!$this->fallbackEnabled) {
            throw $e;
        }

        ($this->reporter)($e);
        $this->disable();
    }
}
