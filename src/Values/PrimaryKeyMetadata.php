<?php

namespace NormCache\Values;

use InvalidArgumentException;

final readonly class PrimaryKeyMetadata
{
    public const INTEGER = 'integer';

    public const STRING = 'string';

    public function __construct(
        public string $column,
        public string $family,
    ) {
        if ($column === '') {
            throw new InvalidArgumentException('Primary-key column must not be empty.');
        }

        if (!in_array($family, [self::INTEGER, self::STRING], true)) {
            throw new InvalidArgumentException('Primary-key family must be integer or string.');
        }
    }

    public function token(mixed $value): ?string
    {
        return $this->family === self::INTEGER
            ? $this->integerToken($value)
            : $this->stringToken($value);
    }

    public function valueFromToken(string $token): int|string|null
    {
        if ($this->family === self::INTEGER) {
            if (!str_starts_with($token, 'i:')) {
                return null;
            }

            $value = substr($token, 2);

            if ($this->integerToken($value) !== $token) {
                return null;
            }

            return (int) $value;
        }

        if (!str_starts_with($token, 's:')) {
            return null;
        }

        $encoded = substr($token, 2);
        $padding = (4 - strlen($encoded) % 4) % 4;
        $decoded = base64_decode(
            strtr($encoded, '-_', '+/') . str_repeat('=', $padding),
            true,
        );

        return is_string($decoded) && $this->stringToken($decoded) === $token
            ? $decoded
            : null;
    }

    private function integerToken(mixed $value): ?string
    {
        if (is_int($value)) {
            return 'i:' . $value;
        }

        if (!is_string($value) || preg_match('/^(?:0|-[1-9][0-9]*|[1-9][0-9]*)$/D', $value) !== 1) {
            return null;
        }

        // Reject values outside the native 64-bit range: PHP's (int) cast saturates
        // rather than erroring, so a mismatch here means the value overflowed.
        if ((string) (int) $value !== $value) {
            return null;
        }

        return 'i:' . $value;
    }

    private function stringToken(mixed $value): ?string
    {
        if (!is_string($value)) {
            return null;
        }

        return 's:' . rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
    }
}
