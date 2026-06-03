<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\MorphMany;
use Illuminate\Database\Eloquent\Relations\MorphOne;
use Illuminate\Database\Eloquent\Relations\MorphToMany;
use Illuminate\Database\Eloquent\SoftDeletes;
use NormCache\Cacheable;

class Post extends Model
{
    use Cacheable;
    use SoftDeletes;

    protected $guarded = [];

    protected $casts = [
        'published' => 'boolean',
        'metadata' => 'array',
    ];

    public function author(): BelongsTo
    {
        return $this->belongsTo(Author::class);
    }

    public function tags(): MorphToMany
    {
        return $this->morphToMany(Tag::class, 'taggable');
    }

    public function comments(): MorphMany
    {
        return $this->morphMany(Comment::class, 'commentable');
    }

    public function latestComment(): MorphOne
    {
        return $this->morphOne(Comment::class, 'commentable');
    }
}
