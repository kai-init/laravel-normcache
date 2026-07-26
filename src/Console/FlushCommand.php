<?php

namespace NormCache\Console;

use Illuminate\Console\Command;
use NormCache\Facades\NormCache;

final class FlushCommand extends Command
{
    protected $signature = 'normcache:flush';

    protected $description = 'Invalidate every NormCache payload by advancing the global epoch.';

    public function handle(): int
    {
        if (!NormCache::flushAll()) {
            $this->error('NormCache global invalidation failed.');

            return self::FAILURE;
        }

        $this->info('NormCache global epoch advanced.');

        return self::SUCCESS;
    }
}
