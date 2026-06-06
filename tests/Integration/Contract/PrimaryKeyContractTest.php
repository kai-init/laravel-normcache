<?php

namespace NormCache\Tests\Integration\Contract;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

/**
 * Contract tests: primary-key lookups (find, whereKey, where id) must return identical
 * results on the native path (withoutCache), cold-cache path, and warm-cache path,
 * particularly regarding result ordering and fast-path bypass.
 */
class PrimaryKeyContractTest extends TestCase
{
    public function test_where_in_primary_key_order_contract(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::create(['name' => 'Carol']);

        $this->contract(
            fn() => Author::whereIn('id', [3, 1, 2])->get(),
            fn() => Author::withoutCache()->whereIn('id', [3, 1, 2])->get(),
        );

        $this->contract(
            fn() => Author::whereIn('id', [2, 3, 1])->get(),
            fn() => Author::withoutCache()->whereIn('id', [2, 3, 1])->get(),
        );
    }

    public function test_where_in_primary_key_with_explicit_order_contract(): void
    {
        Author::create(['name' => 'Alice']);
        Author::create(['name' => 'Bob']);
        Author::create(['name' => 'Carol']);

        $this->contract(
            fn() => Author::whereIn('id', [3, 1, 2])->orderByDesc('id')->get(),
            fn() => Author::withoutCache()->whereIn('id', [3, 1, 2])->orderByDesc('id')->get(),
        );
    }
}
