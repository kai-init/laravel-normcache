<?php

namespace NormCache\Payload;

final class ThroughIndexAdapter implements PayloadAdapter
{
    public function encode(mixed $payload): string
    {
        return json_encode([
            'i' => array_map('strval', $payload['ids']),
            't' => $payload['throughKeys'],
        ], JSON_THROW_ON_ERROR);
    }

    public function decode(mixed $payload): PayloadDecodeResult
    {
        $decoded = is_string($payload) ? json_decode($payload, true) : $payload;

        if (!is_array($decoded)
            || !is_array($decoded['i'] ?? null)
            || !array_is_list($decoded['i'])
            || !is_array($decoded['t'] ?? null)
            || !array_is_list($decoded['t'])
            || count($decoded['i']) !== count($decoded['t'])) {
            return PayloadDecodeResult::corrupt();
        }

        return PayloadDecodeResult::valid([
            'ids' => $decoded['i'],
            'throughKeys' => $decoded['t'],
        ], $decoded['i'] === []);
    }
}
