<?php

namespace NormCache\Tests\Fixtures\Models;

class InstrumentedPost extends Post
{
    protected $table = 'posts';

    protected $connection = 'testing';

    public static int $setRawAttributesCalls = 0;

    public function setRawAttributes($attributes = [], $sync = false)
    {
        static::$setRawAttributesCalls++;

        return parent::setRawAttributes($attributes, $sync);
    }
}
