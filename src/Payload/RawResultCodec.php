<?php

namespace NormCache\Payload;

use NormCache\Support\CacheSerializer;
use NormCache\Values\PrimaryKeyMetadata;
use NormCache\Values\RawResultPayload;

final readonly class RawResultCodec
{
    public function __construct(
        private CacheSerializer $serializer,
    ) {}

    /**
     * @param  list<\stdClass>  $rows
     * @param  array<string, string>  $versions
     */
    public function encode(
        array $rows,
        string $epoch,
        array $versions = [],
        ?string $tagVersion = null,
    ): string {
        ksort($versions, SORT_STRING);

        $nativeRows = [];

        foreach ($rows as $row) {
            $nativeRows[] = (array) $row;
        }

        $envelope = [
            'f' => 4,
            'ep' => $epoch,
            'vec' => $versions,
            'rows' => $nativeRows,
        ];

        if ($tagVersion !== null) {
            $envelope['tv'] = $tagVersion;
        }

        return $this->serializer->encode($envelope);
    }

    public function decode(string $payload): RawResultPayload
    {
        $envelope = $this->serializer->decode($payload);

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== 4
            || !is_string($envelope['ep'] ?? null)
            || !is_array($envelope['vec'] ?? null)
            || !is_array($envelope['rows'] ?? null)
            || (array_key_exists('tv', $envelope) && !is_string($envelope['tv']))
        ) {
            return RawResultPayload::corrupt();
        }

        $versions = $this->stringMap($envelope['vec']);
        $rows = $this->rowList($envelope['rows']);

        if ($versions === null || $rows === null) {
            return RawResultPayload::corrupt();
        }

        return new RawResultPayload(
            valid: true,
            rows: $rows,
            epoch: $envelope['ep'],
            versions: $versions,
            tagVersion: $envelope['tv'] ?? null,
        );
    }

    public function encodeRow(\stdClass $row, string $epoch): string
    {
        return $this->serializer->encode([
            'f' => 4,
            'ep' => $epoch,
            'row' => (array) $row,
        ]);
    }

    public function decodeRow(
        string $payload,
        ?PrimaryKeyMetadata $primaryKey = null,
        ?string $expectedToken = null,
    ): RawResultPayload {
        $envelope = $this->serializer->decode($payload);

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== 4
            || !is_string($envelope['ep'] ?? null)
            || !is_array($envelope['row'] ?? null)
            || !$this->matchesToken($envelope['row'], $primaryKey, $expectedToken)
        ) {
            return RawResultPayload::corrupt();
        }

        return new RawResultPayload(
            valid: true,
            rows: [(object) $envelope['row']],
            epoch: $envelope['ep'],
        );
    }

    public function decodeRowObject(
        string $payload,
        string $expectedEpoch,
        ?PrimaryKeyMetadata $primaryKey = null,
        ?string $expectedToken = null,
    ): ?\stdClass {
        $envelope = $this->serializer->decode($payload);

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== 4
            || ($envelope['ep'] ?? null) !== $expectedEpoch
            || !is_array($envelope['row'] ?? null)
        ) {
            return null;
        }

        if ($primaryKey !== null && $expectedToken !== null) {
            $value = $envelope['row'][$primaryKey->column] ?? null;

            if (
                $primaryKey->family === PrimaryKeyMetadata::INTEGER
                    ? (!is_int($value) && !is_string($value)
                        || substr($expectedToken, 2) !== (string) $value)
                    : (!is_string($value)
                        || $expectedToken !== 's:' . rtrim(strtr(base64_encode($value), '+/', '-_'), '='))
            ) {
                return null;
            }
        }

        return (object) $envelope['row'];
    }

    private function matchesToken(
        array $row,
        ?PrimaryKeyMetadata $primaryKey,
        ?string $expectedToken,
    ): bool {
        if ($primaryKey === null || $expectedToken === null) {
            return true;
        }

        if (!array_key_exists($primaryKey->column, $row)) {
            return false;
        }

        $value = $row[$primaryKey->column];

        if ($primaryKey->family === PrimaryKeyMetadata::INTEGER) {
            return (is_int($value) || is_string($value))
                && str_starts_with($expectedToken, 'i:')
                && substr($expectedToken, 2) === (string) $value;
        }

        return is_string($value)
            && $expectedToken === 's:' . rtrim(strtr(base64_encode($value), '+/', '-_'), '=');
    }

    /** @return array<string, string>|null */
    private function stringMap(array $values): ?array
    {
        $result = [];

        foreach ($values as $key => $value) {
            if (!is_string($key) || !is_string($value)) {
                return null;
            }

            $result[$key] = $value;
        }

        ksort($result, SORT_STRING);

        return $result;
    }

    /** @return list<\stdClass>|null */
    private function rowList(array $rows): ?array
    {
        $result = [];

        foreach ($rows as $row) {
            if (!is_array($row)) {
                return null;
            }

            $result[] = (object) $row;
        }

        return $result;
    }
}
