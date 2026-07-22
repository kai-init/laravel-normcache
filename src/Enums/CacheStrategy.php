<?php

namespace NormCache\Enums;

enum CacheStrategy
{
    case DirectModels;
    case ModelIndex;
    case Result;
    case LiveQuery;
}
