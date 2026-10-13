<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Eloquent\Model;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Comment;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\Integration\Contract\Concerns\CreatesEloquentContractFixtures;
use NormCache\Tests\TestCase;
use ReflectionProperty;

/**
 * Contract tests for Eloquent scope behavior.
 */
final class EloquentScopeContractTest extends TestCase
{
    use CreatesEloquentContractFixtures;

    public function test_global_scope_applies_consistently_cold_and_warm(): void
    {
        $this->fixtures();

        Author::addGlobalScope('contract_test', fn($q) => $q->where('name', '!=', 'Carol'));

        try {
            $this->contract(
                fn() => Author::orderBy('name')->get(),
                fn() => Author::withoutCache()->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Author::class, 'contract_test');
        }
    }

    public function test_without_global_scope_applies_consistently_cold_and_warm(): void
    {
        $this->fixtures();

        Author::addGlobalScope('contract_test2', fn($q) => $q->where('name', '!=', 'Carol'));

        try {
            $this->contract(
                fn() => Author::withoutGlobalScope('contract_test2')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withoutGlobalScope('contract_test2')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Author::class, 'contract_test2');
        }
    }

    public function test_with_count_respects_global_scope_on_related_model(): void
    {
        $this->fixtures(); // Alice: 2 posts (1 published), Bob: 1 post (1 published)

        Post::addGlobalScope('published_only', fn($q) => $q->where('published', true));

        try {
            $this->contract(
                fn() => Author::withCount('posts')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withCount('posts')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Post::class, 'published_only');
        }
    }

    public function test_belongs_to_many_respects_global_scope_on_related(): void
    {
        $this->fixtures(); // Alice has php+laravel tags, Bob has php
        Tag::addGlobalScope('no_laravel', fn($q) => $q->where('name', '!=', 'laravel'));

        try {
            $this->contract(
                fn() => Author::with('tags')->orderBy('name')->get(),
                fn() => Author::withoutCache()->with('tags')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Tag::class, 'no_laravel');
        }
    }

    public function test_with_count_respects_global_scope_column_restriction_on_belongs_to_many(): void
    {
        $this->fixtures();
        Tag::addGlobalScope('no_laravel', fn($q) => $q->where('name', '!=', 'laravel'));

        try {
            $this->contract(
                fn() => Author::withCount('tags')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withCount('tags')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Tag::class, 'no_laravel');
        }
    }

    public function test_has_many_through_respects_global_scope_on_through_model(): void
    {
        $this->fixtures(); // UK country → posts through Alice+Bob; Carol has no country
        Post::addGlobalScope('published', fn($q) => $q->where('published', true));

        try {
            $this->contract(
                fn() => Country::with('posts')->first(),
                fn() => Country::withoutCache()->with('posts')->first(),
            );
        } finally {
            $this->clearGlobalScope(Post::class, 'published');
        }
    }

    public function test_morph_to_excludes_soft_deleted_related_by_default(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete(); // soft-delete the post that c2 points to

        $this->contract(
            fn() => Comment::with('commentable')->orderBy('id')->get(),
            fn() => Comment::withoutCache()->with('commentable')->orderBy('id')->get(),
        );
    }

    public function test_morph_to_includes_soft_deleted_via_constraint(): void
    {
        ['p1' => $p1] = $this->fixtures();
        $p1->delete();

        $this->contract(
            fn() => Comment::with(['commentable' => fn($q) => $q->withoutGlobalScopes()])->orderBy('id')->get(),
            fn() => Comment::withoutCache()->with(['commentable' => fn($q) => $q->withoutGlobalScopes()])->orderBy('id')->get(),
        );
    }

    public function test_with_count_honours_removed_scope_on_parent_builder(): void
    {
        // Aggregate miss-fetch must honour removed scopes so excluded rows receive 0 instead of null.
        Author::addGlobalScope('has_country', fn($q) => $q->whereNotNull('country_id'));

        try {
            $this->fixtures(); // Alice, Bob have country_id=1; Carol has null

            $this->contract(
                fn() => Author::withoutGlobalScope('has_country')->withCount('posts')->orderBy('name')->get(),
                fn() => Author::withoutCache()->withoutGlobalScope('has_country')->withCount('posts')->orderBy('name')->get(),
            );
        } finally {
            $this->clearGlobalScope(Author::class, 'has_country');
        }
    }

    private function clearGlobalScope(string $modelClass, string $name): void
    {
        $prop = new ReflectionProperty(Model::class, 'globalScopes');
        $scopes = $prop->getValue();
        unset($scopes[$modelClass][$name]);
        $prop->setValue(null, $scopes);
    }
}
