<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasManyThrough;
use NormCache\Traits\Cacheable;

// 'reporting'-space model on the countries table.
class ReportingCountry extends Model
{
    use Cacheable;

    protected $table = 'countries';

    protected $guarded = [];

    protected static array $normCacheSpaces = ['reporting'];

    public function spacedPosts(): HasManyThrough
    {
        return $this->hasManyThrough(SpacedPost::class, SpacedAuthor::class, 'country_id', 'author_id');
    }

    public function crossSpacePosts(): HasManyThrough
    {
        return $this->hasManyThrough(SpacedPost::class, ReportingAuthor::class, 'country_id', 'author_id');
    }
}
