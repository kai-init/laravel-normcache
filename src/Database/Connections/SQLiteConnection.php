<?php

namespace NormCache\Database\Connections;

final class SQLiteConnection extends \Illuminate\Database\SQLiteConnection
{
    use BuildsCachingQueries;
}
