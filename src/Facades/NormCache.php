<?php

namespace NormCache\Facades;

use Illuminate\Support\Facades\Facade;

class NormCache extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return 'normcache';
    }
}
