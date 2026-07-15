<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\MorphToMany;
use NormCache\Traits\Cacheable;

// 'content'-space model on the posts table.
class SpacedPost extends Model
{
    use Cacheable;

    protected $table = 'posts';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['content'];

    public function spacedAuthor(): BelongsTo
    {
        return $this->belongsTo(SpacedAuthor::class, 'author_id');
    }

    public function catalogTags(): MorphToMany
    {
        return $this->morphToMany(CatalogTag::class, 'taggable', 'taggables', 'taggable_id', 'tag_id');
    }
}
