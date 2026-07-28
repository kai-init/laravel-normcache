<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
use NormCache\Planning\MutationKeyExtractor;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;

final class MutationKeyExtractorTest extends UnitTestCase
{
    public function test_where_in_produces_a_proven_pk_set(): void
    {
        $query = DB::query()->from('posts')->whereIn('id', [3, 1, 2]);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertSame(['i:1', 'i:2', 'i:3'], $tokens);
    }

    public function test_where_integer_in_raw_produces_a_proven_pk_set(): void
    {
        // Model::destroy([...]) and Eloquent's whereKey() compile an integer-array
        // predicate to this Laravel where-type, not 'In'.
        $query = DB::query()->from('posts')->whereIntegerInRaw('id', [3, 1, 2]);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertSame(['i:1', 'i:2', 'i:3'], $tokens);
    }

    public function test_joined_mutations_are_not_treated_as_precise(): void
    {
        $query = DB::query()
            ->from('posts')
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->where('posts.id', 1);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertNull($tokens);
    }
}
