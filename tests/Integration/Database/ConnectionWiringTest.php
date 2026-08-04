<?php

namespace NormCache\Tests\Integration\Database;

use Illuminate\Database\Connection;
use Illuminate\Support\Facades\DB;
use NormCache\Database\Connections\SQLiteConnection;
use NormCache\Database\QueryBuilder;
use NormCache\Tests\TestCase;

final class ConnectionWiringTest extends TestCase
{
    public function test_db_table_returns_normcache_query_builder(): void
    {
        $builder = DB::table('authors');

        $this->assertInstanceOf(QueryBuilder::class, $builder);
    }

    public function test_connection_returns_normcache_connection_instance(): void
    {
        $connection = DB::connection();

        $this->assertInstanceOf(Connection::class, $connection);
        $this->assertInstanceOf(SQLiteConnection::class, $connection);
    }
}
