<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Collection;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;
use ReflectionProperty;

class QueryCallbackContractTest extends TestCase
{
    public function test_before_query_callback_affects_normalized_cache_key_and_results(): void
    {
        $this->fixtures();

        Author::orderBy('name')->get();

        $query = fn() => Author::orderBy('name')
            ->beforeQuery(fn($base) => $base->where('name', 'Bob'))
            ->get();

        $this->assertSame(['Bob'], $query()->pluck('name')->all());
        $this->assertSame(['Bob'], $query()->pluck('name')->all());
    }

    public function test_before_query_callback_runs_after_global_scopes_like_eloquent(): void
    {
        $this->fixtures();

        Author::addGlobalScope('has_country', fn($builder) => $builder->whereNotNull('country_id'));

        try {
            $cached = Author::orderBy('name')
                ->beforeQuery(fn($base) => $base->orWhere('name', 'Carol'))
                ->get()
                ->pluck('name')
                ->all();

            $native = Author::withoutCache()
                ->orderBy('name')
                ->beforeQuery(fn($base) => $base->orWhere('name', 'Carol'))
                ->get()
                ->pluck('name')
                ->all();

            $this->assertSame($native, $cached);
            $this->assertSame(['Alice', 'Bob', 'Carol'], $cached);
        } finally {
            $this->clearGlobalScope(Author::class, 'has_country');
        }
    }

    public function test_global_scopes_are_applied_once_per_cache_execution(): void
    {
        $this->fixtures();
        $calls = 0;

        Author::addGlobalScope('counted', function ($builder) use (&$calls) {
            $calls++;
            $builder->whereNotNull('country_id');
        });

        try {
            $query = fn() => Author::orderBy('name')->get();

            $this->assertSame(['Alice', 'Bob'], $query()->pluck('name')->all());
            $this->assertSame(['Alice', 'Bob'], $query()->pluck('name')->all());
            $this->assertSame(2, $calls);
        } finally {
            $this->clearGlobalScope(Author::class, 'counted');
        }
    }

    public function test_before_query_callback_affects_result_cache_key_and_results(): void
    {
        $this->fixtures();

        Author::dependsOn([Post::class])->orderBy('name')->get();

        $query = fn() => Author::dependsOn([Post::class])
            ->orderBy('name')
            ->beforeQuery(fn($base) => $base->where('name', 'Bob'))
            ->get();

        $this->assertSame(['Bob'], $query()->pluck('name')->all());
        $this->assertSame(['Bob'], $query()->pluck('name')->all());
    }

    public function test_before_query_callback_affects_scalar_cache_key_and_results(): void
    {
        $this->fixtures();
        $calls = 0;

        $this->assertSame(3, Author::count());

        $query = function () use (&$calls) {
            return Author::query()->beforeQuery(function ($base) use (&$calls) {
                $calls++;
                $base->whereNotNull('country_id');
            })
                ->count();
        };

        $this->assertSame(2, $query());
        $this->assertSame(2, $query());
        $this->assertSame(2, $calls);
    }

    public function test_before_query_callback_runs_once_for_pluck_and_value(): void
    {
        $this->fixtures();
        $calls = 0;

        $pluck = function () use (&$calls) {
            return Author::orderBy('name')
                ->beforeQuery(function ($base) use (&$calls) {
                    $calls++;
                    $base->whereNotNull('country_id');
                })
                ->pluck('name');
        };

        $value = function () use (&$calls) {
            return Author::orderBy('name')
                ->beforeQuery(function ($base) use (&$calls) {
                    $calls++;
                    $base->where('name', 'Bob');
                })
                ->value('name');
        };

        $this->assertSame(['Alice', 'Bob'], $pluck()->all());
        $this->assertSame(['Alice', 'Bob'], $pluck()->all());
        $this->assertSame('Bob', $value());
        $this->assertSame('Bob', $value());
        $this->assertSame(4, $calls);
    }

    public function test_before_query_callback_affects_pagination_count_and_items(): void
    {
        $this->fixtures();

        Author::orderBy('name')->paginate(10);

        $query = fn() => Author::orderBy('name')
            ->beforeQuery(fn($base) => $base->whereNotNull('country_id'))
            ->paginate(10);

        $cold = $query();
        $warm = $query();

        $this->assertSame(2, $cold->total());
        $this->assertSame(['Alice', 'Bob'], $cold->pluck('name')->all());
        $this->assertSame($this->normalize($cold), $this->normalize($warm));
    }

    public function test_before_query_callback_affects_belongs_to_eager_cache_path(): void
    {
        $this->fixtures();
        $calls = 0;

        Post::with('author')->orderBy('title')->get();

        $query = function () use (&$calls) {
            return Post::with([
                'author' => function ($builder) use (&$calls) {
                    $builder->beforeQuery(function ($base) use (&$calls) {
                        $calls++;
                        $base->where('authors.name', 'Bob');
                    });
                },
            ])->orderBy('title')->get();
        };

        $this->assertSame([null, null, 'Bob'], $query()->pluck('author.name')->all());
        $this->assertSame([null, null, 'Bob'], $query()->pluck('author.name')->all());
        $this->assertSame(2, $calls);
    }

    public function test_before_query_callback_affects_pivot_cache_hash_and_results(): void
    {
        ['alice' => $alice] = $this->fixtures();
        $calls = 0;

        $alice->tags()->orderBy('tags.name')->get();

        $query = function () use ($alice, &$calls) {
            return $alice->tags()->orderBy('tags.name')
                ->beforeQuery(function ($base) use (&$calls) {
                    $calls++;
                    $base->where('tags.name', 'php');
                })
                ->get();
        };

        $this->assertSame(['php'], $query()->pluck('name')->all());
        $this->assertSame(['php'], $query()->pluck('name')->all());
        $this->assertSame(2, $calls);
    }

    public function test_pivot_before_query_callback_runs_after_related_global_scopes(): void
    {
        ['alice' => $alice] = $this->fixtures();

        Tag::addGlobalScope('php_only', fn($builder) => $builder->where('tags.name', 'php'));

        try {
            $cached = $alice->tags()
                ->orderBy('tags.name')
                ->beforeQuery(fn($base) => $base->orWhere('tags.name', 'laravel'))
                ->get()
                ->pluck('name')
                ->all();

            $native = $alice->tags()
                ->withoutCache()
                ->orderBy('tags.name')
                ->beforeQuery(fn($base) => $base->orWhere('tags.name', 'laravel'))
                ->get()
                ->pluck('name')
                ->all();

            $this->assertSame($native, $cached);
            $this->assertSame(['laravel', 'php'], $cached);
        } finally {
            $this->clearGlobalScope(Tag::class, 'php_only');
        }
    }

    public function test_before_query_callback_affects_through_cache_hash_and_results(): void
    {
        ['country' => $country] = $this->fixtures();
        $calls = 0;

        $country->posts()->orderBy('posts.title')->get();

        $query = function () use ($country, &$calls) {
            return $country->posts()->orderBy('posts.title')
                ->beforeQuery(function ($base) use (&$calls) {
                    $calls++;
                    $base->where('posts.title', 'B1');
                })
                ->get();
        };

        $this->assertSame(['B1'], $query()->pluck('title')->all());
        $this->assertSame(['B1'], $query()->pluck('title')->all());
        $this->assertSame(2, $calls);
    }

    public function test_after_query_callback_runs_on_normalized_cold_and_warm_results(): void
    {
        $this->fixtures();
        $calls = 0;

        $query = function () use (&$calls) {
            return Author::orderBy('name')
                ->afterQuery(function ($authors) use (&$calls) {
                    $calls++;

                    return $authors->reject(fn($author) => $author->name === 'Bob')->values();
                })
                ->get();
        };

        $this->assertSame(['Alice', 'Carol'], $query()->pluck('name')->all());
        $this->assertSame(['Alice', 'Carol'], $query()->pluck('name')->all());
        $this->assertSame(2, $calls);
        $this->assertSame(['Alice', 'Bob', 'Carol'], Author::orderBy('name')->get()->pluck('name')->all());
    }

    public function test_after_query_callback_does_not_contaminate_aggregate_result_cache(): void
    {
        $this->fixtures();
        $calls = 0;

        $query = function () use (&$calls) {
            return Author::withCount('posts')
                ->orderBy('name')
                ->afterQuery(function ($authors) use (&$calls) {
                    $calls++;

                    return $authors->reject(fn($author) => $author->name === 'Bob')->values();
                })
                ->get();
        };

        $this->assertSame(['Alice', 'Carol'], $query()->pluck('name')->all());
        $this->assertSame(['Alice', 'Carol'], $query()->pluck('name')->all());
        $this->assertSame(2, $calls);
        $this->assertSame(
            ['Alice', 'Bob', 'Carol'],
            Author::withCount('posts')->orderBy('name')->get()->pluck('name')->all()
        );
    }

    public function test_after_query_callback_runs_for_value_and_pluck_without_caching_transformed_values(): void
    {
        $this->fixtures();

        $pluck = fn() => Author::orderBy('name')
            ->afterQuery(fn(Collection $names) => $names->map(fn($name) => strtoupper($name)))
            ->pluck('name');

        $value = fn() => Author::orderBy('name')
            ->afterQuery(function ($authors) {
                $authors->first()?->setAttribute('name', 'CHANGED');

                return $authors;
            })
            ->value('name');

        $this->assertSame(['ALICE', 'BOB', 'CAROL'], $pluck()->all());
        $this->assertSame(['ALICE', 'BOB', 'CAROL'], $pluck()->all());
        $this->assertSame('CHANGED', $value());
        $this->assertSame('CHANGED', $value());
        $this->assertSame(['Alice', 'Bob', 'Carol'], Author::orderBy('name')->pluck('name')->all());
    }

    public function test_after_query_callback_does_not_run_for_aggregate_scalar_operations(): void
    {
        $this->fixtures();
        $calls = 0;

        $query = fn() => Author::query()
            ->afterQuery(function ($result) use (&$calls) {
                $calls++;

                return $result;
            })
            ->count();

        $this->assertSame(3, $query());
        $this->assertSame(3, $query());
        $this->assertSame(0, $calls);
    }

    public function test_after_query_callback_does_not_contaminate_pivot_cache(): void
    {
        ['alice' => $alice] = $this->fixtures();

        $filtered = fn() => $alice->tags()
            ->orderBy('tags.name')
            ->afterQuery(fn($tags) => $tags->reject(fn($tag) => $tag->name === 'php')->values())
            ->get();

        $this->assertSame(['laravel'], $filtered()->pluck('name')->all());
        $this->assertSame(['laravel'], $filtered()->pluck('name')->all());
        $this->assertNotEmpty($this->redisKeys('test:pivot:*'));
        $this->assertSame(
            ['laravel', 'php'],
            $alice->tags()->orderBy('tags.name')->get()->pluck('name')->all()
        );
    }

    public function test_after_query_callback_does_not_contaminate_through_cache(): void
    {
        ['country' => $country] = $this->fixtures();

        $filtered = fn() => $country->posts()
            ->orderBy('posts.title')
            ->afterQuery(fn($posts) => $posts->reject(fn($post) => $post->title === 'B1')->values())
            ->get();

        $this->assertSame(['A1', 'A2'], $filtered()->pluck('title')->all());
        $this->assertSame(['A1', 'A2'], $filtered()->pluck('title')->all());
        $this->assertNotEmpty($this->redisKeys('test:through:*'));
        $this->assertSame(
            ['A1', 'A2', 'B1'],
            $country->posts()->orderBy('posts.title')->get()->pluck('title')->all()
        );
    }

    private function fixtures(): array
    {
        $country = Country::create(['name' => 'UK']);
        $alice = Author::create(['name' => 'Alice', 'country_id' => $country->id]);
        $bob = Author::create(['name' => 'Bob', 'country_id' => $country->id]);
        $carol = Author::create(['name' => 'Carol']);

        Post::create(['title' => 'A1', 'author_id' => $alice->id, 'views' => 10]);
        Post::create(['title' => 'A2', 'author_id' => $alice->id, 'views' => 20]);
        Post::create(['title' => 'B1', 'author_id' => $bob->id, 'views' => 30]);

        $php = Tag::create(['name' => 'php']);
        $laravel = Tag::create(['name' => 'laravel']);
        $alice->tags()->attach([$php->id, $laravel->id]);

        return compact('country', 'alice', 'bob', 'carol', 'php', 'laravel');
    }

    private function clearGlobalScope(string $modelClass, string $name): void
    {
        $property = new ReflectionProperty(Model::class, 'globalScopes');
        $scopes = $property->getValue();
        unset($scopes[$modelClass][$name]);
        $property->setValue(null, $scopes);
    }
}
