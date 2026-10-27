<?php

namespace NormCache\Tests\Unit;

use Illuminate\Support\Facades\DB;
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

    public function test_pk_with_nested_or_filter_preserves_finite_bound(): void
    {
        $query = RawPost::query()->toBase()
            ->where('id', 7)
            ->where(function ($q): void {
                $q->where('title', 'a')->orWhere('title', 'b');
            });

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertSame(['i:7'], $tokens);
    }

    public function test_pk_with_exists_and_not_exists_preserve_finite_bound(): void
    {
        foreach ([false, true] as $notExists) {
            $query = RawPost::query()->toBase()->where('id', 7);
            $callback = static function ($q): void {
                $q->select('authors.id')->from('authors')->whereColumn('authors.id', 'posts.author_id');
            };
            $notExists ? $query->whereNotExists($callback) : $query->whereExists($callback);

            $this->assertSame(['i:7'], (new MutationKeyExtractor)->extract(
                $query,
                new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            ));
        }
    }

    public function test_intersecting_pk_bounds_narrow_to_intersection(): void
    {
        $query = RawPost::query()->toBase()
            ->whereIn('id', [1, 2, 7])
            ->where('id', 7);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertSame(['i:7'], $tokens);

        $query2 = RawPost::query()->toBase()
            ->whereIn('id', [1, 2])
            ->whereIn('id', [2, 3]);

        $this->assertSame(['i:2'], (new MutationKeyExtractor)->extract(
            $query2,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        ));
    }

    public function test_disjoint_pk_bounds_produce_empty_array(): void
    {
        $query = RawPost::query()->toBase()
            ->whereIn('id', [1, 2])
            ->whereIn('id', [3, 4]);

        $tokens = (new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        );

        $this->assertSame([], $tokens);
    }

    public function test_top_level_or_and_raw_fall_back_to_null(): void
    {
        $orQuery = RawPost::query()->toBase()
            ->where('id', 7)
            ->orWhere('title', 'other');

        $this->assertNull((new MutationKeyExtractor)->extract(
            $orQuery,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        ));

        $rawQuery = RawPost::query()->toBase()
            ->where('id', 7)
            ->whereRaw('1 = 1');

        $this->assertNull((new MutationKeyExtractor)->extract(
            $rawQuery,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        ));
    }

    public function test_nested_raw_predicates_and_subquery_raw_components_fall_back_to_null(): void
    {
        $queries = [
            RawPost::query()->toBase()->where('id', 7)->where(function ($q): void {
                $q->whereRaw('1 = 1');
            }),
            RawPost::query()->toBase()->where('id', 7)->where(function ($q): void {
                $q->where(function ($nested): void {
                    $nested->whereRaw('1 = 1');
                });
            }),
        ];

        foreach ([false, true] as $notExists) {
            foreach (['predicate', 'projection'] as $rawPart) {
                $query = RawPost::query()->toBase()->where('id', 7);
                $callback = static function ($q) use ($rawPart): void {
                    $q->from('authors');
                    $rawPart === 'projection' ? $q->selectRaw('1') : $q->select('authors.id')->whereRaw('1 = 1');
                };
                $notExists ? $query->whereNotExists($callback) : $query->whereExists($callback);
                $queries[] = $query;
            }
        }

        foreach ($queries as $query) {
            $this->assertNull((new MutationKeyExtractor)->extract(
                $query,
                new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
            ));
        }
    }

    public function test_exists_raw_from_and_join_targets_fall_back_but_structured_join_is_safe(): void
    {
        $rawFrom = RawPost::query()->toBase()->where('id', 7)->whereExists(static function ($q): void {
            $q->select('authors.id')->from(DB::raw('authors'));
        });
        $rawJoin = RawPost::query()->toBase()->where('id', 7)->whereExists(static function ($q): void {
            $q->select('authors.id')->from('authors')->join(DB::raw('profiles'), 'profiles.author_id', '=', 'authors.id');
        });
        $structuredJoin = RawPost::query()->toBase()->where('id', 7)->whereExists(static function ($q): void {
            $q->select('authors.id')->from('authors')->join('profiles', 'profiles.author_id', '=', 'authors.id');
        });
        $extractor = new MutationKeyExtractor;
        $primaryKey = new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER);

        $this->assertNull($extractor->extract($rawFrom, $primaryKey));
        $this->assertNull($extractor->extract($rawJoin, $primaryKey));
        $this->assertSame(['i:7'], $extractor->extract($structuredJoin, $primaryKey));
    }

    public function test_raw_expression_in_non_primary_key_predicate_falls_back_to_null(): void
    {
        $query = RawPost::query()->toBase()->where('id', 7)->where('title', DB::raw('title'));

        $this->assertNull((new MutationKeyExtractor)->extract(
            $query,
            new PrimaryKeyMetadata('id', PrimaryKeyMetadata::INTEGER),
        ));
    }
}
