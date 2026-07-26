<?php

namespace NormCache\Values;

final readonly class CacheState
{
    /** @param array<string, string> $versions table hash => version, excluding the root outside QUERY_GROUP */
    public function __construct(
        public string $key,
        public string $epoch,
        public string $version,
        public string $generation,
        public array $versions,
        public ?string $tag,
        public ?string $tagKey,
        public string $guard,
    ) {}

    /** Value equality: readonly objects still compare by identity under ===. */
    public function equals(self $other): bool
    {
        return $this == $other;
    }
}
