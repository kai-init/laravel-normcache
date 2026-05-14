<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @mixin \NormCache\CacheManager
 */
class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
