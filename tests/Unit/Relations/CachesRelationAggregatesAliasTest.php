<?php

namespace NormCache\Tests\Unit\Relations;

use Illuminate\Database\Query\Expression;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

class CachesRelationAggregatesAliasTest extends TestCase
{
    private function resolveAlias(mixed $column, ?string $name, ?string $function, mixed $columnArg): string
    {
        $builder = Author::query();
        $method = new \ReflectionMethod($builder, 'resolveAlias');
        $method->setAccessible(true);

        return $method->invoke($builder, $column, $name, $function, $columnArg);
    }

    public function test_reads_alias_from_quoted_sql_suffix(): void
    {
        $column = new Expression('(select count(*) from "posts") as "weird_custom_alias"');

        $alias = $this->resolveAlias($column, 'posts', 'count', '*');

        $this->assertSame('weird_custom_alias', $alias);
    }

    public function test_reads_alias_from_backtick_quoted_sql_suffix(): void
    {
        $column = new Expression('(select count(*) from `posts`) as `weird_custom_alias`');

        $alias = $this->resolveAlias($column, 'posts', 'count', '*');

        $this->assertSame('weird_custom_alias', $alias);
    }

    public function test_falls_back_to_prediction_when_sql_has_no_recognizable_alias(): void
    {
        $column = new Expression('(select count(*) from "posts")');

        $alias = $this->resolveAlias($column, 'posts', 'count', '*');

        $this->assertSame('posts_count', $alias);
    }
}
