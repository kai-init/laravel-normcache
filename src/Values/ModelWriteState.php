<?php

namespace NormCache\Values;

final readonly class ModelWriteState
{
    public function __construct(
        public bool $existed,
        public bool $preInvalidated,
    ) {}
}
