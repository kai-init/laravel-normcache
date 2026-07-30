<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Eloquent\Model;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;

final class CustomHydrationModel extends Model
{
    use Cacheable;

    public static int $newInstanceCalls = 0;

    public \stdClass $marker;

    public function __construct(array $attributes = [])
    {
        $this->marker = new \stdClass;

        parent::__construct($attributes);
    }

    public function newInstance($attributes = [], $exists = false)
    {
        self::$newInstanceCalls++;

        return parent::newInstance($attributes, $exists);
    }
}

/**
 * Contract tests for model hydration after a cached query has been resolved.
 */
final class ModelHydrationContractTest extends TestCase
{
    public function test_custom_model_lifecycle_uses_fresh_laravel_instances(): void
    {
        $source = new CustomHydrationModel;
        CustomHydrationModel::$newInstanceCalls = 0;

        $first = $source->newFromBuilder(['id' => 1]);
        $second = $source->newFromBuilder(['id' => 2]);

        $this->assertSame(2, CustomHydrationModel::$newInstanceCalls);
        $this->assertNotSame($first->marker, $second->marker);
    }

    public function test_eager_loaded_models_match_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Ivy']);
        $post = Post::create(['title' => 'WithComments', 'author_id' => $author->id]);
        Comment::create(['body' => 'Nice post', 'commentable_id' => $post->id, 'commentable_type' => Post::class]);

        $this->contract(
            cached: fn() => Post::with('comments')->whereKey($post->id)->get(),
            native: fn() => Post::withoutCache()->with('comments')->whereKey($post->id)->get(),
        );
    }

    public function test_retrieved_fires_once_per_model_on_cached_and_live_reads(): void
    {
        Author::create(['name' => 'Nia']);
        Author::create(['name' => 'Omar']);

        $seen = [];
        Author::retrieved(function (Author $author) use (&$seen): void {
            $seen[] = $author->name;
        });

        Author::query()->orderBy('id')->get();
        $this->assertSame(['Nia', 'Omar'], $seen);

        $seen = [];
        Author::query()->orderBy('id')->get();
        $this->assertSame(['Nia', 'Omar'], $seen, 'retrieved must fire on a cache hit');

        $seen = [];
        Author::withoutCache()->orderBy('id')->get();
        $this->assertSame(['Nia', 'Omar'], $seen, 'retrieved must fire on a bypassed read');
    }

    public function test_hydration_resolves_the_connection_exactly_as_eloquent_does(): void
    {
        $author = new Author;
        $own = $author->getConnectionName();

        $this->assertSame($own, $author->newFromBuilder(['id' => 1], null)->getConnectionName());
        $this->assertSame($own, $author->newFromBuilder(['id' => 1], '')->getConnectionName());
        $this->assertSame($own, $author->newFromBuilder(['id' => 1], $own)->getConnectionName());
        $this->assertSame('other', $author->newFromBuilder(['id' => 1], 'other')->getConnectionName());

        $author->newFromBuilder(['id' => 1]);
        $author->setConnection('drifted');
        $this->assertSame('drifted', $author->newFromBuilder(['id' => 1])->getConnectionName());

        $author->setTable('relocated');
        $this->assertSame('relocated', $author->newFromBuilder(['id' => 1])->getTable());
    }

    public function test_hydrated_models_expose_the_same_state_as_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Pia']);

        Author::query()->whereKey($author->id)->get();
        $cached = Author::query()->whereKey($author->id)->first();
        $native = Author::withoutCache()->whereKey($author->id)->first();

        $this->assertSame($native->getAttributes(), $cached->getAttributes());
        $this->assertSame($native->getRawOriginal(), $cached->getRawOriginal());
        // Cast originals compare by value; Carbon instances are never identical.
        $this->assertEquals($native->getOriginal(), $cached->getOriginal());
        $this->assertSame($native->exists, $cached->exists);
        $this->assertSame($native->isDirty(), $cached->isDirty());
        $this->assertSame($native->getConnectionName(), $cached->getConnectionName());
        $this->assertSame($native->getTable(), $cached->getTable());
        $this->assertFalse($cached->wasRecentlyCreated);
    }

    public function test_joined_models_with_an_explicit_root_select_match_native_eloquent(): void
    {
        $author = Author::create(['name' => 'Lyle']);
        $post = Post::create(['title' => 'Joined', 'author_id' => $author->id]);
        $this->contract(
            cached: fn() => Post::query()->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.*')
                ->whereKey($post->id)
                ->get(),
            native: fn() => Post::withoutCache()->join('authors', 'authors.id', '=', 'posts.author_id')
                ->select('posts.*')
                ->whereKey($post->id)
                ->get(),
        );
    }
}
