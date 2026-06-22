<?php

namespace NormCache\Tests\Fixtures\Models;

class NewFromBuilderOverridingPost extends Post
{
    protected $table = 'posts';

    public static int $newFromBuilderCalls = 0;

    public function newFromBuilder($attributes = [], $connection = null)
    {
        static::$newFromBuilderCalls++;

        return parent::newFromBuilder($attributes, $connection);
    }
}
