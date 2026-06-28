<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

// Fixture declaring 'content' cache-space membership. Backed by the posts table so
// space wiring can be exercised end-to-end.
class SpacedPost extends Model
{
    use Cacheable;

    protected $table = 'posts';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['content'];
}
