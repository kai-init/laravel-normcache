<?php

namespace NormCache\Values;

/** Build-lock and version-guard state a repository hands back so the store path can write and release. */
final readonly class BuildHandle
{
    public function __construct(
        public ?string $buildingKey = null,
        public ?string $buildingToken = null,
        public ?string $wakeKey = null,
        public array $versionKeys = [],
        public array $expectedVersions = [],
    ) {}
}
