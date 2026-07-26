<?php

namespace NormCache\Payload;

use NormCache\Support\CacheSerializer;
use NormCache\Values\RawResultPayload;
use stdClass;

final readonly class RawResultCodec
{
    public function __construct(
        private CacheSerializer $serializer,
        private NativeRowAdapter $rows,
    ) {}

    /**
     * @param  list<stdClass>  $rows
     * @param  array<string, string>  $versions
     */
    public function encode(
        array $rows,
        string $epoch,
        array $versions = [],
        ?string $tagVersion = null,
    ): string {
        ksort($versions, SORT_STRING);

        $envelope = [
            'f' => 4,
            'ep' => $epoch,
            'vec' => $versions,
            'rows' => array_map($this->rows->toArray(...), $rows),
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

    public function encodeRow(stdClass $row, string $epoch): string
    {
        return $this->serializer->encode([
            'f' => 4,
            'ep' => $epoch,
            'row' => $this->rows->toArray($row),
        ]);
    }

    public function decodeRow(string $payload): RawResultPayload
    {
        $envelope = $this->serializer->decode($payload);

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== 4
            || !is_string($envelope['ep'] ?? null)
            || !is_array($envelope['row'] ?? null)
        ) {
            return RawResultPayload::corrupt();
        }

        return new RawResultPayload(
            valid: true,
            rows: [$this->rows->toObject($envelope['row'])],
            epoch: $envelope['ep'],
        );
    }

    public function decodeRowObject(string $payload, string $expectedEpoch): ?stdClass
    {
        $envelope = $this->serializer->decode($payload);

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== 4
            || ($envelope['ep'] ?? null) !== $expectedEpoch
            || !is_array($envelope['row'] ?? null)
        ) {
            return null;
        }

        return (object) $envelope['row'];
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

    /** @return list<stdClass>|null */
    private function rowList(array $rows): ?array
    {
        $result = [];

        foreach ($rows as $row) {
            if (!is_array($row)) {
                return null;
            }

            $result[] = $this->rows->toObject($row);
        }

        return $result;
    }
}
