<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

class UuidItem extends Model
{
    use Cacheable;

    protected $keyType = 'string';

    public $incrementing = false;

    protected $guarded = [];
}
