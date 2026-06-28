<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// 'content'-space author backed by the authors table, co-located with SpacedPost
// so relation caching across a space can be exercised.
class SpacedAuthor extends Model
{
    use Cacheable;

    protected $table = 'authors';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['content'];
}
