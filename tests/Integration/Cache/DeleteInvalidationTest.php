<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Planning\TableIdentityResolver;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\UuidItem;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use NormCache\Values\TableIdentity;

final class CascadeDeleteNote extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $table = 'cascade_delete_notes';

    protected $guarded = [];
}

final class RestrictedDeleteNote extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $table = 'restricted_delete_notes';

    protected $guarded = [];
}

final class NocaseItem extends Model
{
    use Cacheable;

    public $timestamps = false;

    public $incrementing = false;

    protected $table = 'nocase_items';

    protected $keyType = 'string';

    protected $guarded = [];
}

final class UnavailableDeleteMetadataConnection extends SQLiteConnection
{
    public function getSchemaBuilder()
    {
        throw new \RuntimeException('Schema metadata is unavailable.');
    }
}

final class DeleteInvalidationTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        if (DB::connection()->getDriverName() === 'sqlite') {
            DB::statement('PRAGMA foreign_keys = ON');
        }
    }

    protected function tearDown(): void
    {
        foreach (['cascade_delete_notes', 'restricted_delete_notes', 'nocase_items'] as $table) {
            Schema::dropIfExists($table);
        }

        parent::tearDown();
    }

    private function restrictedNote(): Post
    {
        Schema::create('restricted_delete_notes', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('post_id')->constrained('posts');
            $table->string('body');
        });

        [, $post] = $this->authorAndPost();
        $note = RestrictedDeleteNote::query()->create([
            'post_id' => $post->getKey(),
            'body' => 'Restricted child',
        ]);
        RestrictedDeleteNote::query()->findOrFail($note->getKey());
        RestrictedDeleteNote::query()->where('post_id', $post->getKey())->delete();

        return $post;
    }

    public function test_delete_precisely_invalidates_the_root_and_broadly_invalidates_cascade_children(): void
    {
        [$author, $post] = $this->authorAndPost();
        $unrelated = UuidItem::query()->create(['id' => 'unrelated', 'name' => 'Unrelated']);

        Author::query()->findOrFail($author->getKey());
        Post::query()->findOrFail($post->getKey());
        Post::query()->findOrFail($post->getKey());
        UuidItem::query()->findOrFail($unrelated->getKey());

        $authorTable = $this->identity('authors');
        $postTable = $this->identity('posts');
        $uuidTable = $this->identity('uuid_items');
        $authorGeneration = $this->generation($authorTable);
        $authorVersion = $this->version($authorTable);
        $postGeneration = $this->generation($postTable);
        $uuidGeneration = $this->generation($uuidTable);
        $authorRow = $this->cacheKeys()->row(
            $authorTable,
            $authorGeneration,
            'i:' . $author->getKey(),
        );
        $epoch = $this->epoch();

        $author->delete();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame($authorGeneration, $this->generation($authorTable));
        $this->assertSame((string) ((int) $authorVersion + 1), $this->version($authorTable));
        $this->assertNull($this->cacheStore()->getRaw($authorRow));
        $this->assertSame((string) ((int) $postGeneration + 1), $this->generation($postTable));
        $this->assertSame($uuidGeneration, $this->generation($uuidTable));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Unrelated', UuidItem::query()->findOrFail($unrelated->getKey())->name);
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertNull(Post::query()->find($post->getKey()));
        DB::disableQueryLog();

        $this->assertCount(1, DB::getQueryLog());
    }

    public function test_delete_invalidation_waits_for_commit(): void
    {
        [$author, $post] = $this->authorAndPost();
        Author::query()->findOrFail($author->getKey());
        Post::query()->findOrFail($post->getKey());

        $authorTable = $this->identity('authors');
        $postTable = $this->identity('posts');
        $authorVersion = $this->version($authorTable);
        $postGeneration = $this->generation($postTable);
        $epoch = $this->epoch();

        DB::beginTransaction();
        $author->delete();
        $this->assertSame($authorVersion, $this->version($authorTable));
        $this->assertSame($postGeneration, $this->generation($postTable));
        DB::commit();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame((string) ((int) $authorVersion + 1), $this->version($authorTable));
        $this->assertSame((string) ((int) $postGeneration + 1), $this->generation($postTable));
        $this->assertNull(Post::query()->find($post->getKey()));
    }

    public function test_rolled_back_delete_discards_surgical_invalidation(): void
    {
        [$author, $post] = $this->authorAndPost();
        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);

        $authorTable = $this->identity('authors');
        $postTable = $this->identity('posts');
        $authorVersion = $this->version($authorTable);
        $postGeneration = $this->generation($postTable);
        $epoch = $this->epoch();

        DB::beginTransaction();
        $author->delete();
        DB::rollBack();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame($authorVersion, $this->version($authorTable));
        $this->assertSame($postGeneration, $this->generation($postTable));
        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);
    }

    public function test_unproven_delete_broadly_invalidates_only_its_table(): void
    {
        UuidItem::query()->create(['id' => 'first', 'name' => 'Delete']);
        UuidItem::query()->create(['id' => 'second', 'name' => 'Keep']);
        UuidItem::query()->orderBy('id')->get();

        $table = $this->identity('uuid_items');
        $generation = $this->generation($table);
        $epoch = $this->epoch();

        UuidItem::query()->where('name', 'Delete')->delete();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame((string) ((int) $generation + 1), $this->generation($table));
    }

    public function test_set_null_invalidates_the_changed_child_but_does_not_walk_its_descendants(): void
    {
        $country = Country::query()->create(['name' => 'Parent country']);
        $author = Author::query()->create([
            'name' => 'Author',
            'country_id' => $country->getKey(),
        ]);
        $post = Post::query()->create([
            'title' => 'Unchanged post',
            'author_id' => $author->getKey(),
        ]);

        Author::query()->findOrFail($author->getKey());
        Post::query()->findOrFail($post->getKey());
        Post::query()->findOrFail($post->getKey());

        $authorTable = $this->identity('authors');
        $postTable = $this->identity('posts');
        $authorGeneration = $this->generation($authorTable);
        $postGeneration = $this->generation($postTable);
        $epoch = $this->epoch();

        $country->delete();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame((string) ((int) $authorGeneration + 1), $this->generation($authorTable));
        $this->assertSame($postGeneration, $this->generation($postTable));
        $this->assertNull(Author::query()->findOrFail($author->getKey())->country_id);

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Unchanged post', Post::query()->findOrFail($post->getKey())->title);
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_cascade_dependencies_are_followed_recursively(): void
    {
        Schema::create('cascade_delete_notes', function (Blueprint $table): void {
            $table->id();
            $table->foreignId('post_id')->constrained('posts')->cascadeOnDelete();
            $table->string('body');
        });

        [$author, $post] = $this->authorAndPost();
        $note = CascadeDeleteNote::query()->create([
            'post_id' => $post->getKey(),
            'body' => 'Nested child',
        ]);
        CascadeDeleteNote::query()->findOrFail($note->getKey());

        $noteTable = $this->identity('cascade_delete_notes');
        $noteGeneration = $this->generation($noteTable);
        $epoch = $this->epoch();

        $author->delete();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame(
            (string) ((int) $noteGeneration + 1),
            $this->generation($noteTable),
        );
        $this->assertNull(CascadeDeleteNote::query()->find($note->getKey()));
    }

    public function test_truncate_broadly_invalidates_only_its_table(): void
    {
        [, $post] = $this->authorAndPost();
        UuidItem::query()->create(['id' => 'truncate-me', 'name' => 'Delete']);
        Post::query()->findOrFail($post->getKey());
        UuidItem::query()->get();

        $postTable = $this->identity('posts');
        $uuidTable = $this->identity('uuid_items');
        $postGeneration = $this->generation($postTable);
        $uuidGeneration = $this->generation($uuidTable);
        $epoch = $this->epoch();

        UuidItem::query()->toBase()->truncate();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame($postGeneration, $this->generation($postTable));
        $this->assertSame((string) ((int) $uuidGeneration + 1), $this->generation($uuidTable));

        DB::flushQueryLog();
        DB::enableQueryLog();
        $this->assertSame('Cached child', Post::query()->findOrFail($post->getKey())->title);
        DB::disableQueryLog();

        $this->assertSame([], DB::getQueryLog());
    }

    public function test_truncate_invalidation_survives_a_rolled_back_transaction(): void
    {
        UuidItem::query()->create(['id' => 'truncate-me', 'name' => 'Delete']);
        UuidItem::query()->get();

        $uuidTable = $this->identity('uuid_items');
        $uuidGeneration = $this->generation($uuidTable);
        $epoch = $this->epoch();

        DB::beginTransaction();
        UuidItem::query()->toBase()->truncate();
        $this->assertSame(
            (string) ((int) $uuidGeneration + 1),
            $this->generation($uuidTable),
            'truncate is not rollback-safe on every driver, so it must invalidate before commit',
        );
        DB::rollBack();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame(
            (string) ((int) $uuidGeneration + 1),
            $this->generation($uuidTable),
            'a rollback must not restore cache entries a truncate may already have orphaned',
        );
    }

    public function test_truncate_invalidates_children_whose_foreign_key_does_not_cascade(): void
    {
        $this->restrictedNote();

        $noteTable = $this->identity('restricted_delete_notes');
        $noteGeneration = $this->generation($noteTable);
        $epoch = $this->epoch();

        Post::query()->toBase()->truncate();

        $this->assertSame($epoch, $this->epoch());
        $this->assertSame(
            (string) ((int) $noteGeneration + 1),
            $this->generation($noteTable),
            'truncate cascades to every referencing table on some drivers regardless of the delete action',
        );
    }

    public function test_delete_does_not_invalidate_children_whose_foreign_key_does_not_cascade(): void
    {
        $post = $this->restrictedNote();

        $noteTable = $this->identity('restricted_delete_notes');
        $noteGeneration = $this->generation($noteTable);

        Post::query()->whereKey($post->getKey())->delete();

        $this->assertSame($noteGeneration, $this->generation($noteTable));
    }

    public function test_case_insensitive_string_key_delete_invalidates_the_cached_row(): void
    {
        Schema::create('nocase_items', function (Blueprint $table): void {
            $table->string('id', 36)->collation('nocase')->primary();
            $table->string('name');
        });

        NocaseItem::query()->create(['id' => 'abc', 'name' => 'Cached']);
        $this->assertSame('Cached', NocaseItem::query()->findOrFail('abc')->name);

        $deleted = NocaseItem::query()->where('id', 'ABC')->delete();
        $this->assertSame(1, $deleted, 'the column collation must match the two spellings');

        $this->assertNull(NocaseItem::query()->find('abc'));
    }

    public function test_unavailable_delete_metadata_falls_back_to_the_global_epoch(): void
    {
        $name = 'delete-metadata-unavailable';
        $database = (string) DB::connection()->getDatabaseName();

        config()->set("database.connections.{$name}", [
            'driver' => 'sqlite',
            'database' => $database,
            'prefix' => '',
            'name' => $name,
            'normcache_scope' => $name,
        ]);
        DB::extend($name, static fn(array $config) => new UnavailableDeleteMetadataConnection(
            new \PDO('sqlite:' . $database),
            $database,
            '',
            $config,
        ));
        DB::purge($name);

        try {
            UuidItem::on($name)->create(['id' => 'fallback', 'name' => 'Fallback']);
            $epoch = $this->epoch();

            UuidItem::on($name)->whereKey('fallback')->delete();

            $this->assertSame($epoch + 1, $this->epoch());
        } finally {
            DB::disconnect($name);
            DB::purge($name);
            DB::forgetExtension($name);
        }
    }

    /** @return array{Author, Post} */
    private function authorAndPost(): array
    {
        $author = Author::query()->create(['name' => 'Parent']);
        $post = Post::query()->create([
            'title' => 'Cached child',
            'author_id' => $author->getKey(),
        ]);

        return [$author, $post];
    }

    private function identity(string $table): TableIdentity
    {
        $identity = app(TableIdentityResolver::class)->resolve(DB::connection(), $table);

        if ($identity === null) {
            self::fail("Expected table identity for [{$table}].");
        }

        return $identity;
    }

    private function generation(TableIdentity $table): string
    {
        return $this->cacheStore()->getRaw($this->cacheKeys()->generation($table)) ?? '0';
    }

    private function version(TableIdentity $table): string
    {
        return $this->cacheStore()->getRaw($this->cacheKeys()->version($table)) ?? '0';
    }

    private function epoch(): int
    {
        return (int) ($this->cacheStore()->getRaw($this->cacheKeys()->epoch()) ?? '0');
    }
}
