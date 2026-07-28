<?php

namespace NormCache\Database\Connections;

final class MySqlConnection extends \Illuminate\Database\MySqlConnection
{
    use BuildsCachingQueries;
}
