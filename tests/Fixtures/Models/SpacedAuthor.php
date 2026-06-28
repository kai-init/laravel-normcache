<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// 'content'-space model on the authors table, co-located with SpacedPost.
class SpacedAuthor extends Model
{
    use Cacheable;

    protected $table = 'authors';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['content'];
}
