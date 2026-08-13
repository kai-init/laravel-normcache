<?php

namespace NormCache\Exceptions;

final class TableInvalidationException extends \RuntimeException
{
    public function __construct(public readonly int $stateIndex, \Exception $previous)
    {
        parent::__construct('NormCache table invalidation failed.', previous: $previous);
    }
}
