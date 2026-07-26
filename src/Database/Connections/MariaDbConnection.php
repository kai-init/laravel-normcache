<?php

namespace NormCache\Database\Connections;

final class MariaDbConnection extends \Illuminate\Database\MariaDbConnection
{
    use BuildsCachingQueries;
}
