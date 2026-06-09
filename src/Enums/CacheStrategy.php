<?php

namespace NormCache\Enums;

enum CacheStrategy
{
    case DirectModels;
    case NormalizedQuery;
    case VersionedResult;
    case LiveQuery;
}
