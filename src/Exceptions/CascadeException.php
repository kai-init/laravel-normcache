<?php

namespace NormCache\Exceptions;

final class CascadeException extends \RuntimeException
{
    public function __construct(
        public readonly string $stage,
        public readonly string $driver,
        public readonly string $connection,
        public readonly ?string $childReference = null,
        public readonly ?string $parentReference = null,
        public readonly ?string $unexpectedAction = null,
        ?\Throwable $previous = null,
    ) {
        $context = array_filter([
            "driver={$driver}",
            "connection={$connection}",
            $childReference === null ? null : "child={$childReference}",
            $parentReference === null ? null : "parent={$parentReference}",
            $unexpectedAction === null ? null : "action={$unexpectedAction}",
        ]);

        parent::__construct(
            "Cascade metadata resolution failed at [{$stage}] (" . implode(', ', $context) . ').',
            0,
            $previous,
        );
    }

    /**
     * @return array{
     *     stage: string,
     *     driver: string,
     *     connection: string,
     *     child: ?string,
     *     parent: ?string,
     *     action: ?string,
     *     exception: \Throwable
     * }
     */
    public function context(): array
    {
        return [
            'stage' => $this->stage,
            'driver' => $this->driver,
            'connection' => $this->connection,
            'child' => $this->childReference,
            'parent' => $this->parentReference,
            'action' => $this->unexpectedAction,
            'exception' => $this->getPrevious() ?? $this,
        ];
    }

    /** @return list<string> */
    public function fingerprint(): array
    {
        return array_values(array_filter([
            $this->stage,
            $this->driver,
            $this->connection,
            $this->childReference,
            $this->parentReference,
            $this->unexpectedAction,
        ], static fn(?string $value): bool => $value !== null));
    }
}
