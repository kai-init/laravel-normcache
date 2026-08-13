<?php

namespace NormCache\Console;

use Illuminate\Console\Command;
use NormCache\Facades\NormCache;
use NormCache\Values\CacheConfig;

final class DisableCommand extends Command
{
    protected $signature = 'normcache:disable';

    protected $description = 'Stop serving and invalidating NormCache across every node until normcache:enable.';

    public function handle(CacheConfig $config): int
    {
        if (!$config->enabled) {
            $this->warn('NormCache is already disabled by configuration. Nothing changed.');

            return self::SUCCESS;
        }

        if (!NormCache::disableCache()) {
            $this->error('NormCache could not be disabled. The cache is still active.');

            return self::FAILURE;
        }

        $this->info('NormCache disabled. Reads bypass to the database.');

        return self::SUCCESS;
    }
}
