<?php

namespace NormCache\Enums;

enum MutationType: string
{
    case INSERT = 'insert';

    case UPDATE = 'update';

    case UPSERT = 'upsert';

    case DELETE = 'delete';

    case TRUNCATE = 'truncate';
}
