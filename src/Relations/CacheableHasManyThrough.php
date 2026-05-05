<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\HasManyThrough;

class CacheableHasManyThrough extends HasManyThrough
{
    use CachesOneOrManyThrough;
}
