<?php

namespace NormCache\Tests\Integration;

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

    public function test_query_grammar_and_processor_are_usable_on_custom_connection(): void
    {
        $connection = DB::connection();

        $grammar = $connection->getQueryGrammar();
        $this->assertNotNull($grammar);

        $processor = $connection->getPostProcessor();
        $this->assertNotNull($processor);

        $sql = $grammar->compileSelect(DB::table('authors'));
        $this->assertIsString($sql);
        $this->assertNotEmpty($sql);
    }

    public function test_schema_grammar_is_initialized_and_usable_on_custom_connection(): void
    {
        $schema = DB::connection()->getSchemaBuilder();
        $this->assertNotNull($schema);

        $this->assertTrue($schema->hasTable('authors'));
    }

    public function test_driver_specific_methods_pass_through_on_custom_connection(): void
    {
        $connection = DB::connection();

        $this->assertSame('sqlite', $connection->getDriverName());
        $this->assertNotEmpty($connection->getDatabaseName());
        $this->assertSame('testing', $connection->getName());
        $this->assertNotNull($connection->getPdo());
        $this->assertNotNull($connection->getReadPdo());
    }
}
