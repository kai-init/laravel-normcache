<?php

namespace NormCache\Cache;

use NormCache\Support\CacheKeyBuilder;
use NormCache\Support\RedisStore;
use NormCache\Values\CacheConfig;
use NormCache\Values\RuntimeState;

final readonly class CacheSwitch
{
    public function __construct(
        private CacheConfig $config,
        private RuntimeState $runtime,
        private RedisStore $store,
        private CacheKeyBuilder $keys,
    ) {}

    public function readable(): bool
    {
        if (!$this->config->enabled) {
            return false;
        }

        if (!$this->runtime->available()) {
            return true;
        }

        try {
            return !$this->runtime->state(fn(): array => $this->readPair())[1];
        } catch (\Throwable $exception) {
            $this->runtime->fail($exception);

            return false;
        }
    }

    public function invalidating(): bool
    {
        if (!$this->config->enabled) {
            return false;
        }

        if (!$this->runtime->available()) {
            return true;
        }

        try {
            return !$this->runtime->runtimeDisabled(fn(): bool => $this->readFlag());
        } catch (\Throwable) {
            return true;
        }
    }

    public function epoch(): string
    {
        $known = $this->runtime->knownEpoch();

        if ($known !== null) {
            return $known;
        }

        return $this->runtime->epoch(fn(): array => $this->readPair());
    }

    /** @return array{0: string, 1: bool} */
    private function readPair(): array
    {
        $epochKey = $this->keys->epoch();
        $disabledKey = $this->keys->disabled();
        $values = $this->store->mget([$epochKey, $disabledKey]);

        return [
            $values[$epochKey] ?? '0',
            ($values[$disabledKey] ?? null) !== null,
        ];
    }

    private function readFlag(): bool
    {
        return $this->store->getRaw($this->keys->disabled()) !== null;
    }
}
