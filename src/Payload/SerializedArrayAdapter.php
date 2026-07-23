<?php

namespace NormCache\Payload;

use NormCache\Support\RedisStore;

final class SerializedArrayAdapter implements PayloadAdapter
{
    public function __construct(private readonly RedisStore $store) {}

    public function encode(mixed $payload): string
    {
        return $this->store->serialize($payload);
    }

    public function decode(mixed $payload): PayloadDecodeResult
    {
        $decoded = $this->store->unserialize($payload);

        return is_array($decoded)
            ? PayloadDecodeResult::valid($decoded, $decoded === [])
            : PayloadDecodeResult::corrupt();
    }
}
