<?php

namespace NormCache\Tests\Unit;

use Illuminate\Database\Eloquent\Model;
use NormCache\Support\CacheKeyBuilder;
use NormCache\Tests\TestCase;

class CacheKeyBuilderTest extends TestCase
{
    public function test_class_key_rejects_connection_name_containing_colon(): void
    {
        $model = new class extends Model
        {
            protected $connection = 'tenant:7';
        };

        $keys = new CacheKeyBuilder;

        $this->expectException(\InvalidArgumentException::class);

        $keys->classKey($model::class);
    }
}
