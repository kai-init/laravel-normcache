<?php

namespace NormCache\Tests\Unit;

use NormCache\Cache\QueryHashResolver;
use NormCache\Tests\TestCase;

final class QueryHashResolverTest extends TestCase
{
    public function test_it_resolves_the_hash_once_on_demand(): void
    {
        $calls = 0;
        $resolver = new QueryHashResolver(function () use (&$calls): string {
            $calls++;

            return 'query-hash';
        });

        $first = $resolver->value();

        $this->assertSame(1, $calls);
        $this->assertSame($first, $resolver->value());
        $this->assertSame(1, $calls);
    }
}
