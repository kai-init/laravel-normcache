<?php

namespace NormCache\Values;

// A named cache space mapped to one raw Redis hash tag (e.g. "nc", "nc:content").
final readonly class CacheSpace
{
    public function __construct(
        public string $name,
        public string $hashTag,
    ) {}
}
