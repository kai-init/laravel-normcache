<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\HasOneThrough;

class CacheableHasOneThrough extends HasOneThrough
{
    use CachesOneOrManyThrough;
}
