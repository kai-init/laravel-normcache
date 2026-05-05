<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\MorphToMany;
use NormCache\Traits\NormCacheable;

class Tag extends Model
{
    use NormCacheable;

    protected $guarded = [];

    public function posts(): MorphToMany
    {
        return $this->morphedByMany(Post::class, 'taggable');
    }

    public function authors(): MorphToMany
    {
        return $this->morphedByMany(Author::class, 'taggable');
    }
}
