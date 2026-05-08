<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\SoftDeletes;

class UncachedPost extends Model
{
    use SoftDeletes;

    protected $table = 'posts';
    protected $guarded = [];

    protected $casts = [
        'published' => 'boolean',
        'metadata'  => 'array',
    ];

    public function author(): BelongsTo
    {
        return $this->belongsTo(UncachedAuthor::class, 'author_id');
    }
}
