<?php

namespace NormCache\Support;

final class CacheSerializer
{
    private bool $igbinary;

    public function __construct()
    {
        $this->igbinary = extension_loaded('igbinary');
    }

    public function serialize(mixed $value): mixed
    {
        if ((is_int($value) || is_float($value)) && is_finite((float) $value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_serialize($value) : serialize($value);
    }

    public function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return str_contains((string) $value, '.') ? (float) $value : (int) $value;
        }

        if (is_string($value) && isset($value[0]) && $value[0] === "\x00") {
            return $this->igbinary ? igbinary_unserialize($value) : null;
        }

        if (is_string($value) && preg_match('/^[sidbaOCRrN]:|^[sidbaOCRrN];/', $value)) {
            try {
                return unserialize($value);
            } catch (\Throwable) {
                return $value;
            }
        }

        return $value;
    }

    public function unserializeMany(array $raw): array
    {
        return array_map(
            fn($value) => $value !== null && $value !== false ? $this->unserialize($value) : null,
            $raw
        );
    }
}
