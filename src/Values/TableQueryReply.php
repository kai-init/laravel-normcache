<?php

namespace NormCache\Values;

final readonly class TableQueryReply
{
    public function __construct(
        public string $kind,
        public string $version,
        public ?string $generation,
        public ?string $payload,
        public bool $isOwner,
        public ?string $ownerToken,
    ) {}
}
