<?php

namespace NormCache\Support;

use Throwable;

final readonly class CacheSerializer
{
    private bool $igbinary;

    public function __construct(?bool $igbinary = null)
    {
        $this->igbinary = $igbinary ?? extension_loaded('igbinary');

        if ($this->igbinary && !extension_loaded('igbinary')) {
            throw new \RuntimeException('The igbinary codec was requested but the extension is unavailable.');
        }
    }

    public static function native(): self
    {
        return new self(extension_loaded('igbinary'));
    }

    public function encode(mixed $value): string
    {
        return $this->igbinary
            ? igbinary_serialize($value)
            : serialize($value);
    }

    public function decode(string $payload): mixed
    {
        try {
            return $this->igbinary
                ? @igbinary_unserialize($payload)
                : @unserialize($payload, ['allowed_classes' => false]);
        } catch (Throwable) {
            return null;
        }
    }
}
