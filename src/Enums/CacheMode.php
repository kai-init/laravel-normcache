<?php

namespace NormCache\Enums;

enum CacheMode
{
    case Normalized;
    case Result;
    case Bypass;
}
