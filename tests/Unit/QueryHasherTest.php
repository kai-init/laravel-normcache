<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Query\Builder;
use NormCache\Support\QueryHasher;
use NormCache\Tests\TestCase;

class QueryHasherTest extends TestCase
{
    private function makeBuilder(): Builder
    {
        return $this->app['db']->query();
    }

    public function test_same_query_produces_identical_hash(): void
    {
        $a = $this->makeBuilder()->from('posts')->where('id', 1);
        $b = $this->makeBuilder()->from('posts')->where('id', 1);

        $this->assertSame(QueryHasher::fromQuery($a), QueryHasher::fromQuery($b));
    }

    public function test_different_bindings_produce_different_hash(): void
    {
        $a = $this->makeBuilder()->from('posts')->where('id', 1);
        $b = $this->makeBuilder()->from('posts')->where('id', 2);

        $this->assertNotSame(QueryHasher::fromQuery($a), QueryHasher::fromQuery($b));
    }

    public function test_different_sql_produces_different_hash(): void
    {
        $a = $this->makeBuilder()->from('posts')->where('id', 1);
        $b = $this->makeBuilder()->from('authors')->where('id', 1);

        $this->assertNotSame(QueryHasher::fromQuery($a), QueryHasher::fromQuery($b));
    }

    public function test_it_hashes_raw_string(): void
    {
        $hash = QueryHasher::hash('some data');
        $this->assertIsString($hash);
        $this->assertEquals(16, strlen($hash));
        $this->assertSame(hash('xxh3', 'some data'), $hash);
    }
}
