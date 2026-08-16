<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redis;
use NormCache\Payload\ChangeRecordCodec;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Values\CacheConfig;
use NormCache\Values\ChangeRecord;

final class ChangeRecordTest extends TestCase
{
    private int $authorId;

    protected function setUp(): void
    {
        parent::setUp();

        $this->authorId = (int) Author::query()->create(['name' => 'Author'])->getKey();
    }

    private function seedPosts(int $count): void
    {
        for ($index = 1; $index <= $count; $index++) {
            RawPost::query()->toBase()->insert([
                'title' => 'Post ' . $index,
                'views' => 0,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        }
    }

    private function changeRecordKey(string $table, string $version): string
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $table);

        $this->assertNotNull($identity);

        return $this->cacheKeys()->changeRecord($identity, $version);
    }

    private function readChangeRecord(string $table, string $version): ChangeRecord
    {
        $payload = $this->cacheStore()->getRaw($this->changeRecordKey($table, $version));

        return $payload === null
            ? ChangeRecord::corrupt()
            : $this->app->make(ChangeRecordCodec::class)->decode($payload);
    }

    private function ttlOf(string $key): int
    {
        return (int) Redis::connection('normcache-test')->ttl($key);
    }

    private function currentVersion(string $table): string
    {
        $identity = $this->app->make(TableIdentityResolver::class)
            ->resolve(DB::connection(), $table);

        $this->assertNotNull($identity);

        return (string) ($this->cacheStore()->getRaw($this->cacheKeys()->version($identity)) ?? '0');
    }

    public function test_a_precise_update_records_its_assigned_columns(): void
    {
        $this->seedPosts(3);

        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);

        $record = $this->readChangeRecord('posts', $this->currentVersion('posts'));

        $this->assertTrue($record->valid);
        $this->assertSame('update', $record->mutation);
        $this->assertTrue($record->precise);
        $this->assertContains('title', $record->columns);
    }

    public function test_eloquent_timestamps_reach_the_change_record(): void
    {
        $this->seedPosts(3);

        $post = RawPost::query()->findOrFail(1);
        $post->title = 'changed';
        $post->save();

        $record = $this->readChangeRecord('posts', $this->currentVersion('posts'));

        $this->assertTrue($record->valid);
        $this->assertEqualsCanonicalizing(['title', 'updated_at'], $record->columns);
    }

    public function test_a_base_builder_update_records_no_timestamp(): void
    {
        $this->seedPosts(3);

        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'changed']);

        $this->assertSame(
            ['title'],
            $this->readChangeRecord('posts', $this->currentVersion('posts'))->columns,
        );
    }

    public function test_a_delete_writes_no_record(): void
    {
        $this->seedPosts(3);

        RawPost::query()->toBase()->where('id', 1)->delete();

        $this->assertFalse($this->readChangeRecord('posts', $this->currentVersion('posts'))->valid);
    }

    public function test_a_broad_update_writes_no_record(): void
    {
        $this->seedPosts(3);

        RawPost::query()->toBase()->where('title', 'like', 'Post%')->update(['title' => 'x']);

        $this->assertFalse($this->readChangeRecord('posts', $this->currentVersion('posts'))->valid);
    }

    public function test_a_transaction_mixing_an_update_and_an_insert_writes_no_record(): void
    {
        $this->seedPosts(3);

        DB::transaction(function (): void {
            RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);
            RawPost::query()->toBase()->insert([
                'title' => 'new',
                'views' => 0,
                'published' => true,
                'author_id' => $this->authorId,
                'created_at' => now(),
                'updated_at' => now(),
            ]);
        });

        $this->assertFalse($this->readChangeRecord('posts', $this->currentVersion('posts'))->valid);
    }

    public function test_a_transaction_of_updates_unions_their_columns(): void
    {
        $this->seedPosts(3);

        DB::transaction(function (): void {
            RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);
            RawPost::query()->toBase()->where('id', 2)->update(['views' => 7]);
        });

        $record = $this->readChangeRecord('posts', $this->currentVersion('posts'));

        $this->assertTrue($record->valid);
        $this->assertSame('update', $record->mutation);
        $this->assertEqualsCanonicalizing(['title', 'views'], $record->columns);
    }

    public function test_change_records_expire(): void
    {
        $this->seedPosts(3);

        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);

        $this->assertGreaterThan(
            0,
            $this->ttlOf($this->changeRecordKey('posts', $this->currentVersion('posts'))),
        );
    }

    public function test_change_records_track_the_query_ttl_not_a_longer_entry_ttl(): void
    {
        config()->set('normcache.query_ttl', 60);
        $this->app->forgetInstance(CacheConfig::class);
        $this->app->forgetScopedInstances();

        $this->seedPosts(3);

        RawPost::query()->toBase()->where('published', true)->orderBy('id')->ttl(600)->get();

        $entryKeys = $this->cacheQueryKeysWithField('m');
        $this->assertNotSame([], $entryKeys);

        RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);

        $this->assertEqualsWithDelta(600, $this->ttlOf($entryKeys[0]), 2);
        $this->assertEqualsWithDelta(
            60,
            $this->ttlOf($this->changeRecordKey('posts', $this->currentVersion('posts'))),
            2,
        );
    }

    public function test_a_json_path_assignment_records_its_base_column(): void
    {
        $this->seedPosts(3);

        RawPost::query()->toBase()->where('id', 1)->update(['metadata->flag' => 'x']);

        $record = $this->readChangeRecord('posts', $this->currentVersion('posts'));

        $this->assertTrue($record->valid);
        $this->assertSame(['metadata'], $record->columns);
    }

    public function test_a_record_is_written_per_table_in_a_multi_table_transaction(): void
    {
        $this->seedPosts(3);

        DB::transaction(function (): void {
            RawPost::query()->toBase()->where('id', 1)->update(['title' => 'x']);
            Author::query()->toBase()->where('id', $this->authorId)->update(['name' => 'y']);
        });

        $posts = $this->readChangeRecord('posts', $this->currentVersion('posts'));
        $authors = $this->readChangeRecord('authors', $this->currentVersion('authors'));

        $this->assertTrue($posts->valid);
        $this->assertSame(['title'], $posts->columns);

        $this->assertTrue($authors->valid);
        $this->assertSame(['name'], $authors->columns);
    }
}
