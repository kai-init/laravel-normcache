<?php

namespace NormCache\Console;

use Illuminate\Console\Command;
use NormCache\Facades\NormCache;
use NormCache\Traits\NormCacheable;

class FlushCommand extends Command
{
    protected $signature = 'normcache:flush
        {--model= : Fully-qualified class name of the model to flush}';

    protected $description = 'Flush the normcache. Flushes all entries unless --model is specified.';

    public function handle(): int
    {
        $model = $this->option('model');

        return $model ? $this->flushModel($model) : $this->flushAll();
    }

    private function flushAll(): int
    {
        $count = NormCache::flushAll();

        $this->info("Flushed {$count} model cache key(s).");

        return Command::SUCCESS;
    }

    private function flushModel(string $model): int
    {
        if (!class_exists($model)) {
            $this->error("Class [{$model}] does not exist.");

            return Command::FAILURE;
        }

        if (!in_array(NormCacheable::class, class_uses_recursive($model), true)) {
            $this->error("Class [{$model}] does not use the Cacheable trait.");

            return Command::FAILURE;
        }

        NormCache::flushModel($model);

        $this->info("Cache flushed for [{$model}].");

        return Command::SUCCESS;
    }
}
