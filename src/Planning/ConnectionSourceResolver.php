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

        if ($source === '') {
            return null;
        }

        // A search_path decides which schema an unqualified PostgreSQL source
        // resolves to, so two tenants sharing a connection name must not share
        // a table identity. An explicit normcache_scope overrides this above.
        $searchPath = $config['search_path'] ?? $config['schema'] ?? null;

        if (is_array($searchPath)) {
            $searchPath = implode(',', $searchPath);
        }

        return is_string($searchPath) && $searchPath !== ''
            ? $source . "\0" . $searchPath
            : $source;
    }
}
