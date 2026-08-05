<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\FailureReporter;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;

final class CacheRuntime
{
    private bool $available = true;

    private int $readBypassDepth = 0;

    private ?string $epoch = null;

    private ?bool $runtimeDisabled = null;

    public function __construct(
        private readonly CacheConfig $config,
        private readonly RedisStore $store,
        private readonly CacheKeyBuilder $keys,
        private readonly FailureReporter $failures,
    ) {}

    public function readable(): bool
    {
        if ($this->readBypassDepth > 0 || !$this->config->enabled || !$this->available) {
            return false;
        }

        try {
            return !$this->resolveState()[1];
        } catch (\Throwable $exception) {
            $this->fail($exception);

            return false;
        }
    }

    public function invalidating(): bool
    {
        if (!$this->config->enabled) {
            return false;
        }

        if (!$this->available) {
            return true;
        }

        try {
            return !$this->resolveDisabled();
        } catch (\Throwable) {
            return true;
        }
    }

    public function epoch(): string
    {
        return $this->epoch ?? $this->resolveState()[0];
    }

    public function knownEpoch(): ?string
    {
        return $this->epoch;
    }

    public function rememberEpoch(string $epoch): void
    {
        $this->epoch ??= $epoch;
    }

    public function forgetEpoch(): void
    {
        $this->epoch = null;
        $this->runtimeDisabled = null;
    }

    public function withoutCache(callable $callback): mixed
    {
        $this->readBypassDepth++;

        try {
            return $callback();
        } finally {
            $this->readBypassDepth--;
        }
    }

    public function available(): bool
    {
        return $this->available;
    }

    public function disable(): void
    {
        $this->available = false;
    }

    public function fail(\Throwable $exception): void
    {
        $this->disable();
        $this->failures->cacheUnavailable($exception);
    }

    /** @return array{0: string, 1: bool} */
    private function resolveState(): array
    {
        if ($this->epoch === null) {
            $epochKey = $this->keys->epoch();
            $disabledKey = $this->keys->disabled();
            $values = $this->store->mget([$epochKey, $disabledKey]);
            $this->epoch = $values[$epochKey] ?? '0';
            $this->runtimeDisabled ??= ($values[$disabledKey] ?? null) !== null;
        }

        return [$this->epoch, $this->runtimeDisabled ?? false];
    }

    private function resolveDisabled(): bool
    {
        if ($this->runtimeDisabled === true) {
            return $this->runtimeDisabled = $this->readFlag();
        }

        return $this->runtimeDisabled ??= $this->readFlag();
    }

    private function readFlag(): bool
    {
        return $this->store->getRaw($this->keys->disabled()) !== null;
    }
}
