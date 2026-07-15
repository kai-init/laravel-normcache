<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

class ReportingAuthor extends Model
{
    use Cacheable;

    protected $table = 'authors';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['reporting'];
}
