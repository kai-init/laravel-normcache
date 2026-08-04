<?php

namespace NormCache\Support;

final readonly class CacheSerializer
{
    private const AUTO = 'auto';

    private const PHP = 'php';

    private const IGBINARY = 'igbinary';

    private const PHP_MARKER = 'P';

    private const IGBINARY_MARKER = 'I';

    private string $serializer;

    private bool $igbinaryAvailable;

    public function __construct(string $serializer = self::AUTO)
    {
        $this->igbinaryAvailable = extension_loaded('igbinary');

        if (!in_array($serializer, [self::AUTO, self::PHP, self::IGBINARY], true)) {
            throw new \InvalidArgumentException(
                'NormCache serializer must be auto, php, or igbinary.',
            );
        }

        if ($serializer === self::IGBINARY && !$this->igbinaryAvailable) {
            throw new \RuntimeException('The igbinary codec was requested but the extension is unavailable.');
        }

        $this->serializer = $serializer === self::AUTO
            ? ($this->igbinaryAvailable ? self::IGBINARY : self::PHP)
            : $serializer;
    }

    public static function native(): self
    {
        return new self(self::AUTO);
    }

    public function encode(mixed $value): string
    {
        return $this->serializer === self::IGBINARY
            ? self::IGBINARY_MARKER . igbinary_serialize($value)
            : self::PHP_MARKER . serialize($value);
    }

    public function decode(string $payload): mixed
    {
        $marker = $payload[0] ?? '';

        return match ($marker) {
            self::PHP_MARKER => $this->decodePhp(substr($payload, 1)),
            self::IGBINARY_MARKER => $this->igbinaryAvailable
                ? $this->decodeIgbinary(substr($payload, 1))
                : null,
            default => $this->decodeLegacy($payload),
        };
    }

    private function decodeLegacy(string $payload): mixed
    {
        if ($this->serializer === self::IGBINARY) {
            return $this->decodeIgbinary($payload) ?? $this->decodePhp($payload);
        }

        return $this->decodePhp($payload)
            ?? ($this->igbinaryAvailable ? $this->decodeIgbinary($payload) : null);
    }

    private function decodePhp(string $payload): mixed
    {
        try {
            $value = @unserialize($payload, ['allowed_classes' => false]);

            return $value === false && $payload !== 'b:0;' ? null : $value;
        } catch (\Throwable) {
            return null;
        }
    }

    private function decodeIgbinary(string $payload): mixed
    {
        if (!$this->igbinaryAvailable) {
            return null;
        }

        try {
            return @igbinary_unserialize($payload);
        } catch (\Throwable) {
            return null;
        }
    }
}
