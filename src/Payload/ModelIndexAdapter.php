<?php

namespace NormCache\Payload;

final class ModelIndexAdapter implements PayloadAdapter
{
    public function encode(mixed $payload): string
    {
        return json_encode(array_map('strval', $payload), JSON_THROW_ON_ERROR);
    }

    public function decode(mixed $payload): PayloadDecodeResult
    {
        $ids = is_string($payload) ? json_decode($payload, true) : $payload;

        if (!is_array($ids) || !array_is_list($ids)) {
            return PayloadDecodeResult::corrupt();
        }

        return PayloadDecodeResult::valid($ids, $ids === []);
    }
}
