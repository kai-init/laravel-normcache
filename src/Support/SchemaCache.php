<?php

namespace NormCache\Support;

use Illuminate\Database\Connection;
use NormCache\Planning\ConnectionSourceResolver;
use NormCache\Values\TableIdentity;

final class SchemaCache
{
    private const TTL = 86400;

    /** @var \WeakMap<Connection, array{signature: string, values: array<string, array>}> */
    private \WeakMap $connections;

    public function __construct(private readonly RedisStore $store, private readonly CacheKeyBuilder $keys)
    {
        $this->connections = new \WeakMap;
    }

    /** @param callable(): array $inspect */
    public function remember(Connection $connection, string $epoch, string $kind, callable $inspect): array
    {
        $scope = ConnectionSourceResolver::resolve($connection);

        if ($scope === null) {
            throw new \UnexpectedValueException('Schema metadata requires a stable database source.');
        }

        $signature = TableIdentity::encodeFields([
            TableIdentity::FORMAT,
            $scope,
            (string) $connection->getDriverName(),
            (string) $connection->getDatabaseName(),
            $connection->getTablePrefix(),
            serialize($connection->getConfig('search_path') ?? $connection->getConfig('schema')),
            $epoch,
        ]);
        $cached = $this->connections[$connection] ?? null;

        if ($cached === null || $cached['signature'] !== $signature) {
            $cached = ['signature' => $signature, 'values' => []];
        }

        if (isset($cached['values'][$kind])) {
            return $cached['values'][$kind];
        }

        $key = $this->keys->schema(hash('xxh128', $signature), $kind);
        $raw = $this->store->getRaw($key);
        $values = $raw === null ? null : json_decode($raw, true);

        if (!is_array($values)) {
            $values = $inspect();
            $this->store->setRaw($key, json_encode($values, JSON_THROW_ON_ERROR), self::TTL);
        }

        $cached['values'][$kind] = $values;
        $this->connections[$connection] = $cached;

        return $values;
    }
}
