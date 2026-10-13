<?php

namespace NormCache\Tests\Integration\Contract\Concerns;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;

trait CreatesEloquentContractFixtures
{
    private function fixtures(): array
    {
        $country = Country::create(['name' => 'UK']);

        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $bob = Author::create(['name' => 'Bob',   'country_id' => $country->id]);
        $carol = Author::create(['name' => 'Carol']);

        $p1 = Post::create(['title' => 'A1', 'author_id' => $alice->id, 'views' => 10, 'published' => true]);
        $p2 = Post::create(['title' => 'A2', 'author_id' => $alice->id, 'views' => 20, 'published' => false]);
        $p3 = Post::create(['title' => 'B1', 'author_id' => $bob->id,   'views' => 30, 'published' => true]);

        $php = Tag::create(['name' => 'php']);
        $laravel = Tag::create(['name' => 'laravel']);

        $alice->tags()->attach([$php->id, $laravel->id]);
        $bob->tags()->attach($php->id);

        $p1->tags()->attach($php->id);

        $c1 = Comment::create(['body' => 'Great!',   'commentable_type' => Author::class, 'commentable_id' => $alice->id]);
        $c2 = Comment::create(['body' => 'Nice post', 'commentable_type' => Post::class,  'commentable_id' => $p1->id]);

        return compact('country', 'alice', 'bob', 'carol', 'p1', 'p2', 'p3', 'php', 'laravel', 'c1', 'c2');
    }
}
