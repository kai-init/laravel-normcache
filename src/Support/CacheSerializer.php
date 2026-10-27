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

    public function encode(mixed $value): string
    {
        return $this->serializer === self::IGBINARY
            ? self::IGBINARY_MARKER . igbinary_serialize($value)
            : self::PHP_MARKER . serialize($value);
    }

    public function decode(string $payload): mixed
    {
        try {
            $body = substr($payload, 1);
            $marker = $payload[0] ?? '';

            if ($marker === self::PHP_MARKER) {
                $value = @unserialize($body, ['allowed_classes' => false]);

                return $value === false && $body !== 'b:0;' ? null : $value;
            }

            return $marker === self::IGBINARY_MARKER && $this->igbinaryAvailable
                ? @igbinary_unserialize($body)
                : null;
        } catch (\Throwable) { // @phpstan-ignore catch.neverThrown (custom error handlers may throw)
            return null;
        }
    }
}
