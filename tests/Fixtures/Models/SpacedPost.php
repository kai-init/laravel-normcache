<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use NormCache\Traits\Cacheable;

// Fixture declaring 'content' cache-space membership. Backed by the posts table so
// space wiring can be exercised end-to-end.
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
}
