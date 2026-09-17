<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

final class RawPost extends Model
{
    use Cacheable;

    protected $table = 'posts';

    protected $guarded = [];
}
