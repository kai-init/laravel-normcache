<?php

namespace NormCache\Tests\Unit;

use NormCache\Planning\MutationKeyExtractor;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\UnitTestCase;
use NormCache\Values\PrimaryKeyMetadata;

final class MutationKeyExtractorTest extends UnitTestCase
{
    public function test_where_in_produces_a_proven_pk_set(): void
    {
        $query = RawPost::query()->toBase()->whereIn('id', [3, 1, 2]);

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
        $query = RawPost::query()->toBase()->whereIntegerInRaw('id', [3, 1, 2]);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertSame(['i:1', 'i:2', 'i:3'], $tokens);
    }

    public function test_joined_mutations_are_not_treated_as_precise(): void
    {
        $query = RawPost::query()->toBase()
            ->join('authors', 'authors.id', '=', 'posts.author_id')
            ->where('posts.id', 1);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertNull($tokens);
    }

    public function test_string_primary_key_mutation_extracts_old_and_assigned_tokens(): void
    {
        $query = RawPost::query()->toBase()->where('uuid_items.id', 'old-id');
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::STRING);

        $tokens = (new MutationKeyExtractor)->extractMutation(
            $query,
            $primaryKey,
            ['id' => 'new-id'],
        );
        $expected = [
            $primaryKey->token('old-id'),
            $primaryKey->token('new-id'),
        ];
        sort($expected, SORT_STRING);

        $this->assertSame($expected, $tokens);
    }

    public function test_string_where_in_accepts_uuid_ulid_and_large_keys(): void
    {
        $values = [
            'b8f8702c-4734-45e0-a548-18e3c66f6f9c',
            '01J0QZ5J8Y5RWV2M1Y6N7P8Q9R',
            str_repeat('large-key-', 128),
        ];
        $query = RawPost::query()->toBase()->whereIn('uuid_items.id', $values);
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::STRING);
        $expected = array_map($primaryKey->token(...), $values);
        sort($expected, SORT_STRING);

        $this->assertSame(
            $expected,
            (new MutationKeyExtractor)->extract($query, $primaryKey),
        );
    }
}
