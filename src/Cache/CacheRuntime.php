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

    private ?float $epochReadAt = null;

    private ?string $schemaEpoch = null;

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
        return $this->epoch !== null && !$this->epochExpired()
            ? $this->epoch
            : $this->resolveState()[0];
    }

    public function knownEpoch(): ?string
    {
        return $this->epoch;
    }

    public function rememberEpoch(string $epoch): void
    {
        if ($this->epoch === null) {
            $this->epoch = $epoch;
            $this->epochReadAt = microtime(true);
        }
    }

    public function forgetEpoch(): void
    {
        $this->epoch = null;
        $this->epochReadAt = null;
        $this->runtimeDisabled = null;
    }

    private function epochExpired(): bool
    {
        $interval = $this->config->epochRefreshSeconds;

        return $interval > 0
            && $this->epochReadAt !== null
            && (microtime(true) - $this->epochReadAt) >= $interval;
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

    public function schemaEpoch(): string
    {
        if ($this->schemaEpoch !== null) {
            return $this->schemaEpoch;
        }

        $this->resolveState();

        return $this->schemaEpoch ??= $this->store->getRaw($this->keys->schemaEpoch()) ?? '0';
    }

    public function rememberSchemaEpoch(string $epoch): void
    {
        $this->schemaEpoch = $epoch;
    }

    public function forgetSchemaEpoch(): void
    {
        $this->schemaEpoch = null;
    }

    /** @return array{0: string, 1: bool} */
    private function resolveState(): array
    {
        if ($this->epoch === null || $this->epochExpired()) {
            $epochKey = $this->keys->epoch();
            $disabledKey = $this->keys->disabled();
            $schemaEpochKey = $this->keys->schemaEpoch();
            $values = $this->store->mget([$epochKey, $disabledKey, $schemaEpochKey]);
            $this->epoch = $values[$epochKey] ?? '0';
            $this->epochReadAt = microtime(true);
            $this->runtimeDisabled ??= ($values[$disabledKey] ?? null) !== null;
            $this->schemaEpoch ??= $values[$schemaEpochKey] ?? '0';
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
