<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use NormCache\Traits\Cacheable;

class Comment extends Model
{
    use Cacheable;

    protected $guarded = [];

    public function commentable(): MorphTo
    {
        return $this->morphTo();
    }
}
