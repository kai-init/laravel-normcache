<?php

namespace NormCache\Enums;

enum CacheKind: string
{
    case Model = 'model';
    case ModelIndex = 'model-index';
    case RelationIndex = 'relation-index';
    case Result = 'result';
    case Version = 'version';
}
