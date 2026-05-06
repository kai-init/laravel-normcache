<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\Relations\HasManyThrough;
use Illuminate\Database\Eloquent\Relations\HasOneThrough;
use NormCache\Traits\Cacheable;

class Country extends Model
{
    use Cacheable;

    protected $guarded = [];

    public function authors(): HasMany
    {
        return $this->hasMany(Author::class);
    }

    public function posts(): HasManyThrough
    {
        return $this->hasManyThrough(Post::class, Author::class);
    }

    public function latestPost(): HasOneThrough
    {
        return $this->hasOneThrough(Post::class, Author::class)->latestOfMany();
    }
}
