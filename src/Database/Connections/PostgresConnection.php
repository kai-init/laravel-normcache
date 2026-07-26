<?php

namespace NormCache\Database\Connections;

final class PostgresConnection extends \Illuminate\Database\PostgresConnection
{
    use BuildsCachingQueries;
}
