<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

/**
 * Final contract verification: complex relation queries with raw expressions
 * and custom selects must maintain perfect hydration parity.
 */
class RelationCorrectnessContractTest extends TestCase
{
    private function fixtures(): void
    {
        $country = Country::create(['name' => 'USA']);

        $author = Author::create(['name' => 'John', 'country_id' => $country->id]);
        
        $p1 = Post::create(['title' => 'P1', 'author_id' => $author->id, 'views' => 10]);
        $p2 = Post::create(['title' => 'P2', 'author_id' => $author->id, 'views' => 20]);

        $fiction = Tag::create(['name' => 'fiction']);
        $science = Tag::create(['name' => 'science']);

        $author->tags()->attach([$fiction->id, $science->id]);
    }

    public function test_belongs_to_many_with_where_raw(): void
    {
        $this->fixtures();

        $query = fn() => Author::with(['tags' => fn($q) => $q->whereRaw('LOWER(name) = ?', ['fiction'])])->get();
        $nativeQuery = fn() => Author::withoutCache()->with(['tags' => fn($q) => $q->whereRaw('LOWER(name) = ?', ['fiction'])])->get();

        $this->contract($query, $nativeQuery);
    }

    public function test_has_many_through_with_custom_select_raw(): void
    {
        $this->fixtures();

        $query = fn() => Country::with(['posts' => fn($q) => $q->select('posts.*', DB::raw('posts.id * 2 as doubled_id'))])->get();
        $nativeQuery = fn() => Country::withoutCache()->with(['posts' => fn($q) => $q->select('posts.*', DB::raw('posts.id * 2 as doubled_id'))])->get();

        $this->contract($query, $nativeQuery);

        // Also verify the custom attribute is present and correct
        $warm = $query();
        $this->assertCount(1, $warm);
        $posts = $warm->first()->posts;
        $this->assertCount(2, $posts);
        foreach ($posts as $post) {
            $this->assertEquals($post->id * 2, $post->doubled_id);
        }
    }
}
