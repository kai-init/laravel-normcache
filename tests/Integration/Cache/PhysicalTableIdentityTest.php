<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;

final class PhysicalTableIdentityTest extends TestCase
{
    public function test_aliases_share_rows_and_invalidation_across_prefix_spellings(): void
    {
        $base = config('database.connections.testing');
        config([
            'database.connections.review_prefixed' => array_replace($base, [
                'prefix' => 'review_',
                'normcache_scope' => 'same-physical-database',
            ]),
            'database.connections.review_plain' => array_replace($base, [
                'prefix' => '',
                'normcache_scope' => 'same-physical-database',
            ]),
        ]);
        DB::connection('review_plain')->statement('create table review_authors (id integer primary key, name varchar(100))');
        $prefixed = (new Author)->setConnection('review_prefixed');
        $plain = (new Author)->setConnection('review_plain')->setTable('review_authors');

        try {
            $plain->newQuery()->toBase()->insert(['id' => 1, 'name' => 'Before']);
            $this->assertSame('Before', $prefixed->newQuery()->find(1)->name);
            $plain->newQuery()->toBase()->where('id', 1)->update(['name' => 'After']);
            $this->assertSame('After', $prefixed->newQuery()->find(1)->name);
            $this->assertWarmCacheHit(fn() => $plain->newQuery()->find(1));

            $prefixed->newQuery()->toBase()->where('id', 1)->update(['name' => 'Final']);
            $this->assertSame('Final', $plain->newQuery()->find(1)->name);
            $this->assertWarmCacheHit(fn() => $prefixed->newQuery()->find(1));
            $this->cacheStore()->delete($this->cacheKeysMatching(':r:g'));
            $this->assertSame('Final', $prefixed->newQuery()->orderBy('id')->first()->name);
        } finally {
            DB::connection('review_plain')->statement('drop table review_authors');
        }
    }
}
