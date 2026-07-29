<?php

namespace NormCache\Tests\Integration;

use Illuminate\Support\Facades\DB;
use NormCache\Tests\TestCase;

final class PrimaryKeyMetadataPlanningTest extends TestCase
{
    public function test_scalar_result_cache_path_does_not_introspect_primary_key_metadata(): void
    {
        $this->cacheManager()->clearSchemaMetadata();
        DB::flushQueryLog();
        DB::enableQueryLog();

        $this->assertSame(0, DB::table('posts')->count());

        DB::disableQueryLog();
        $queries = strtolower(implode("\n", array_column(DB::getQueryLog(), 'query')));

        $this->assertStringNotContainsString('pragma_index_list', $queries);
        $this->assertStringNotContainsString('pragma_table_xinfo', $queries);
    }

    public function test_canonical_path_still_resolves_primary_key_metadata(): void
    {
        $this->cacheManager()->clearSchemaMetadata();
        DB::flushQueryLog();
        DB::enableQueryLog();

        $this->assertSame([], DB::table('posts')->get()->all());

        DB::disableQueryLog();
        $queries = strtolower(implode("\n", array_column(DB::getQueryLog(), 'query')));

        $this->assertStringContainsString('pragma_index_list', $queries);
        $this->assertStringContainsString('pragma_table_xinfo', $queries);
    }
}
