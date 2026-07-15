<?php

namespace NormCache\Tests\Unit\Spaces;

use NormCache\Spaces\CacheSpaceRegistry;
use NormCache\Spaces\CacheSpaceResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\UnitTestCase;

class CacheSpaceResolverTest extends UnitTestCase
{
    private function resolver(): CacheSpaceResolver
    {
        return new CacheSpaceResolver(new CacheSpaceRegistry);
    }

    public function test_undeclared_model_resolves_to_default(): void
    {
        $this->assertSame('default', $this->resolver()->resolve(Author::class, null)->name);
    }

    public function test_declared_model_resolves_to_its_first_space(): void
    {
        // SpacedPost declares ['content'].
        $this->assertSame('content', $this->resolver()->resolve(SpacedPost::class, null)->name);
    }

    public function test_explicit_member_space_is_used(): void
    {
        $this->assertSame('content', $this->resolver()->resolve(SpacedPost::class, 'content')->name);
    }

    public function test_explicit_non_member_space_throws(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->resolver()->resolve(SpacedPost::class, 'reporting');
    }
}
