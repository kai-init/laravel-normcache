<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// 'catalog'-space model on the tags table.
class CatalogTag extends Model
{
    use Cacheable;

    protected $table = 'tags';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['catalog'];
}
