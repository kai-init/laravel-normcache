<?php

namespace NormCache\Tests\Fixtures\Models;

use Illuminate\Database\Eloquent\Model;
use NormCache\Traits\Cacheable;

final class VolatilePost extends Model
{
    use Cacheable;

    protected $table = 'posts';

    protected $guarded = [];

    /** @var list<string> */
    protected array $volatileColumns = ['published', 'title_length'];
}
