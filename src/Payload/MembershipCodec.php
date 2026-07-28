<?php

namespace NormCache\Payload;

use JsonException;
use NormCache\Values\MembershipPayload;

final class MembershipCodec
{
    /**
     * @param  list<string>  $ids
     * @param  array<string, string>  $versions
     */
    public function encode(
        string $epoch,
        string $generation,
        array $ids,
        array $versions = [],
        ?string $tagVersion = null,
    ): string {
        ksort($versions, SORT_STRING);

        $envelope = [
            'f' => 4,
            'ep' => $epoch,
            'g' => $generation,
            'ids' => $ids,
            'vec' => $versions,
        ];

        if ($tagVersion !== null) {
            $envelope['tv'] = $tagVersion;
        }

        return json_encode($envelope, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES);
    }

    public function decode(string $payload): MembershipPayload
    {
        try {
            $envelope = json_decode($payload, true, flags: JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return MembershipPayload::corrupt();
        }

        if (
            !is_array($envelope)
            || ($envelope['f'] ?? null) !== 4
            || !is_string($envelope['ep'] ?? null)
            || !is_string($envelope['g'] ?? null)
            || !is_array($envelope['ids'] ?? null)
            || !is_array($envelope['vec'] ?? null)
            || (array_key_exists('tv', $envelope) && !is_string($envelope['tv']))
        ) {
            return MembershipPayload::corrupt();
        }

        $ids = array_values($envelope['ids']);
        $versions = [];

        foreach ($ids as $id) {
            if (!is_string($id)) {
                return MembershipPayload::corrupt();
            }
        }

        foreach ($envelope['vec'] as $key => $value) {
            if (!is_string($key) || !is_string($value)) {
                return MembershipPayload::corrupt();
            }

            $versions[$key] = $value;
        }

        ksort($versions, SORT_STRING);

        return new MembershipPayload(
            valid: true,
            ids: $ids,
            epoch: $envelope['ep'],
            generation: $envelope['g'],
            versions: $versions,
            tagVersion: $envelope['tv'] ?? null,
        );
    }
}
