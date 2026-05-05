<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\MorphToMany;

class CacheableMorphToMany extends MorphToMany
{
    use InvalidatesPivotCache;
}
