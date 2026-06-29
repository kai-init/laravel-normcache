<?php

namespace NormCache\Values;

final readonly class BuildContext
{
    public function __construct(
        public string $queryKey,
        public string $buildingKey,
        public string $lockToken,
        public string $wakeKey,
        public array $versionKeys,
        public array $expectedVersions,
    ) {}
}
