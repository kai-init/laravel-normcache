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
        if (is_int($value)) {
            return $value;
        }

        return $this->igbinary ? igbinary_serialize($value) : serialize($value);
    }

    public function unserialize(mixed $value): mixed
    {
        if (is_numeric($value)) {
            return str_contains((string) $value, '.') ? (float) $value : (int) $value;
        }

        if (!is_string($value)) {
            return $value;
        }

        if (isset($value[0]) && $value[0] === "\x00") {
            return $this->igbinary ? igbinary_unserialize($value) : null;
        }

        if (isset($value[1]) && ($value[1] === ':' || $value[1] === ';')) {
            return unserialize($value);
        }

        return $value;
    }

    public function unserializeMany(array $raw): array
    {
        $values = [];

        foreach ($raw as $key => $value) {
            $values[$key] = $value !== null && $value !== false
                ? $this->unserialize($value)
                : null;
        }

        return $values;
    }
}
