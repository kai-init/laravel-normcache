<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\BelongsToMany;

class CacheableBelongsToMany extends BelongsToMany
{
    use CachesPivotRelation, InvalidatesPivotCache;
}
