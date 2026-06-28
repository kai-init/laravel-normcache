<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// 'reporting'-space model on the countries table.
class ReportingCountry extends Model
{
    use Cacheable;

    protected $table = 'countries';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['reporting'];
}
