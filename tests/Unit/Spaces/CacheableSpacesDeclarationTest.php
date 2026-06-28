<?php

namespace NormCache\Tests\Unit\Spaces;

use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\SpacedPost;
use NormCache\Tests\TestCase;

class CacheableSpacesDeclarationTest extends TestCase
{
    public function test_model_without_declaration_returns_empty(): void
    {
        $this->assertSame([], Author::normCacheSpaces());
    }

    public function test_model_with_declaration_returns_its_spaces(): void
    {
        $this->assertSame(['content'], SpacedPost::normCacheSpaces());
    }
}
