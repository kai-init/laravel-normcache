<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;

abstract class AbstractComment extends Model
{
    protected $table = 'comments';

    protected $guarded = [];
}
