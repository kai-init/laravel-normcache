<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// Multi-space model on the posts table used to verify invalidation fan-out.
class MultiSpacePost extends Model
{
    use Cacheable;

    protected $table = 'posts';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['content', 'reporting'];
}
