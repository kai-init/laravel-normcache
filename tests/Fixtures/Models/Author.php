<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\BelongsToMany;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Database\Eloquent\Relations\HasOne;
use Illuminate\Database\Eloquent\Relations\MorphMany;
use NormCache\Cacheable;

class Author extends Model
{
    use Cacheable;

    protected $guarded = [];

    public function country(): BelongsTo
    {
        return $this->belongsTo(Country::class);
    }

    public function posts(): HasMany
    {
        return $this->hasMany(Post::class);
    }

    public function tags(): BelongsToMany
    {
        return $this->belongsToMany(Tag::class, 'author_tag');
    }

    public function firstPost(): HasOne
    {
        return $this->hasOne(Post::class);
    }

    public function comments(): MorphMany
    {
        return $this->morphMany(Comment::class, 'commentable');
    }

    public function latestPost(): HasOne
    {
        return $this->hasOne(Post::class)->latestOfMany();
    }

    public function mostViewedPost(): HasOne
    {
        return $this->hasOne(Post::class)->ofMany('views', 'MAX');
    }

    public function uncachedPosts(): HasMany
    {
        return $this->hasMany(UncachedPost::class, 'author_id');
    }
}
