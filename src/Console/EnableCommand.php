<?php

namespace NormCache\Console;

use Illuminate\Console\Command;
use NormCache\Facades\NormCache;
use NormCache\Values\CacheConfig;

final class EnableCommand extends Command
{
    protected $signature = 'normcache:enable';

    protected $description = 'Resume NormCache after a runtime disable, advancing the epoch first.';

    public function handle(CacheConfig $config): int
    {
        if (!$config->enabled) {
            $this->warn('NormCache is disabled by configuration. Set NORMCACHE_ENABLED=true to use this command.');

            return self::FAILURE;
        }

        $epoch = NormCache::enableCache();

        if ($epoch === null) {
            $this->error('NormCache could not be enabled. The cache remains disabled.');

            return self::FAILURE;
        }

        $this->info("NormCache enabled at epoch {$epoch}.");

        return self::SUCCESS;
    }
}
