<?php

namespace NormCache\Database\Connections;

final class SqlServerConnection extends \Illuminate\Database\SqlServerConnection
{
    use BuildsCachingQueries;
}
