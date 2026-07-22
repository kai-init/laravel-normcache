<?php

namespace NormCache\Payload;

interface PayloadAdapter
{
    public function encode(mixed $payload): string;

    public function decode(mixed $payload): PayloadDecodeResult;
}
