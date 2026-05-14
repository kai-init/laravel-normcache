<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class UncachedAuthor extends Model
{
    protected $table = 'authors';

    protected $guarded = [];

    public function posts(): HasMany
    {
        return $this->hasMany(UncachedPost::class, 'author_id');
    }
}
