<?php

namespace NormCache\Payload;

use stdClass;

final class NativeRowAdapter
{
    /** @return array<string, scalar|null> */
    public function toArray(stdClass $row): array
    {
        return get_object_vars($row);
    }

    /** @param array<string, scalar|null> $row */
    public function toObject(array $row): stdClass
    {
        return (object) $row;
    }
}
