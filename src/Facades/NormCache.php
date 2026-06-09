<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;
use NormCache\CacheManager;

/**
 * @mixin CacheManager
 *
 * @method static \NormCache\Cache\ExecutionEngine engine()
 * @method static \NormCache\Cache\ResultExecutor result()
 */
class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
