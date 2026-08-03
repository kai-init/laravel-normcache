<?php

namespace NormCache\Planning;

use Illuminate\Database\Connection;

final class ConnectionSourceResolver
{
    public static function resolve(Connection $connection): ?string
    {
        $config = $connection->getConfig();
        $configured = $config['normcache_scope'] ?? null;

        if ($configured !== null) {
            return is_string($configured) && $configured !== ''
                ? $configured
                : null;
        }

        $source = (string) ($config['name'] ?? '');

        return $source === '' ? null : $source;
    }
}
