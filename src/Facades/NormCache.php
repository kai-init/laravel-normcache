<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;
use NormCache\CacheManager;

/**
 * @mixin CacheManager
 *
 * @method static \NormCache\Cache\CacheExecutor executor()
 */
class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
