<?php

namespace NormCache\Values;

use stdClass;

final readonly class RawResultPayload
{
    /** @param list<stdClass> $rows
     * @param  array<string, string>  $versions
     */
    public function __construct(
        public bool $valid,
        public array $rows = [],
        public ?string $epoch = null,
        public array $versions = [],
        public ?string $tagVersion = null,
    ) {}

    public static function corrupt(): self
    {
        return new self(false);
    }
}
