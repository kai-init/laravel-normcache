<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\SQLiteConnection;
use NormCache\Database\QueryStatement;
use NormCache\Tests\TestCase;

final class QueryStatementTest extends TestCase
{
    public function test_it_resolves_sql_and_bindings_once_on_demand(): void
    {
        $calls = 0;
        $statement = new QueryStatement(function () use (&$calls): array {
            $calls++;

            return ['select * from "authors" where "id" = ?', [1]];
        });

        $this->assertSame(0, $calls);
        $this->assertSame('select * from "authors" where "id" = ?', $statement->sql());
        $this->assertSame(1, $calls);
        $this->assertSame([1], $statement->bindings());
        $this->assertSame(1, $calls);
    }

    public function test_it_prepares_bindings_once_after_resolution(): void
    {
        $calls = 0;
        $connection = new SQLiteConnection(new \PDO('sqlite::memory:'));
        $statement = new QueryStatement(function () use (&$calls): array {
            $calls++;

            return ['select ?', [now()]];
        });

        $prepared = $statement->preparedBindings($connection);

        $this->assertSame(1, $calls);
        $this->assertSame($prepared, $statement->preparedBindings($connection));
        $this->assertSame(1, $calls);
    }
}
