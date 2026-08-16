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

        $source = (string) $connection->getName();

        if ($source === '') {
            return null;
        }

        // PostgreSQL search_path is part of an unqualified table's identity.
        $searchPath = $config['search_path'] ?? $config['schema'] ?? null;

        if (is_array($searchPath)) {
            $searchPath = implode(',', $searchPath);
        }

        return is_string($searchPath) && $searchPath !== ''
            ? $source . "\0" . $searchPath
            : $source;
    }
}
