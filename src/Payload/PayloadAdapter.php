<?php

namespace NormCache\Payload;

interface PayloadAdapter
{
    public function encode(mixed $payload): string;

    public function decode(mixed $payload): PayloadDecodeResult;

    // Payload shape differs per adapter, so a generic count() can't work.
    public function cardinality(mixed $payload): ?int;
}
