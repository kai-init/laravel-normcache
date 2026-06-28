<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// Fixture declaring cache-space membership for registry tests. Never queried —
// only its static normCacheSpaces() declaration is read.
class SpacedPost extends Model
{
    use Cacheable;

    protected $guarded = [];

    protected static array $normCacheSpaces = ['content'];
}
