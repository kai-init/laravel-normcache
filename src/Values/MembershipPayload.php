<?php

namespace NormCache\Values;

final readonly class MembershipPayload
{
    /** @param list<string> $ids
     * @param  array<string, string>  $versions
     */
    public function __construct(
        public bool $valid,
        public array $ids = [],
        public ?string $epoch = null,
        public ?string $generation = null,
        public array $versions = [],
        public ?string $tagVersion = null,
    ) {}

    public static function corrupt(): self
    {
        return new self(false);
    }
}
