<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\Country;
use NormCache\Tests\Fixtures\Models\Post;
use NormCache\Tests\Fixtures\Models\Tag;
use NormCache\Tests\TestCase;

class ConnectionAwareCachingTest extends TestCase
{
    public function test_on_connection_queries_use_isolated_model_cache_namespaces(): void
    {
        $this->seedDifferentAuthorsOnTwoConnections();

        $primary = Author::find(1);
        $secondaryBuilder = Author::on('secondary_testing');
        $this->assertSame('secondary_testing', $secondaryBuilder->getModel()->getConnectionName());
        $secondary = $secondaryBuilder->find(1);

        $this->assertSame('Primary Alice', $primary?->name);
        $this->assertSame('Secondary Alice', $secondary?->name);
        $this->assertSame('secondary_testing', $secondary?->getConnectionName());

        $manager = $this->cacheManager();
        $defaultKey = $manager->keys()->classKey(Author::class);
        $secondaryKey = $manager->keys()->classKey(Author::class, 'secondary_testing');
        $defaultVersion = $manager->currentVersion(Author::class);
        $secondaryVersion = $manager->currentVersion(Author::class, 'secondary_testing');

        $this->assertSame(
            'Primary Alice',
            $manager->store()->get($manager->keys()->modelPrefix($defaultKey, $defaultVersion) . '1')['name'] ?? null,
        );
        $this->assertSame(
            'Secondary Alice',
            $manager->store()->get($manager->keys()->modelPrefix($secondaryKey, $secondaryVersion) . '1')['name'] ?? null,
        );

        DB::connection('secondary_testing')->flushQueryLog();
        DB::connection('secondary_testing')->enableQueryLog();

        try {
            $this->assertSame('Secondary Alice', Author::on('secondary_testing')->find(1)?->name);
            $this->assertSame([], DB::connection('secondary_testing')->getQueryLog());
        } finally {
            DB::connection('secondary_testing')->disableQueryLog();
        }
    }

    public function test_on_connection_query_does_not_poison_default_cache_namespace(): void
    {
        $this->seedDifferentAuthorsOnTwoConnections();

        $this->assertSame('Secondary Alice', Author::on('secondary_testing')->find(1)?->name);
        $this->assertSame('Primary Alice', Author::find(1)?->name);
    }

    public function test_normalized_and_scalar_caches_are_connection_scoped(): void
    {
        $this->seedDifferentAuthorsOnTwoConnections();
        DB::connection('secondary_testing')->table('authors')->insert([
            'id' => 2,
            'name' => 'Secondary Bob',
            'country_id' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ]);

        $this->assertSame(['Primary Alice'], Author::orderBy('id')->get()->pluck('name')->all());
        $this->assertSame(
            ['Secondary Alice', 'Secondary Bob'],
            Author::on('secondary_testing')->orderBy('id')->get()->pluck('name')->all(),
        );
        $this->assertSame(1, Author::count());
        $this->assertSame(2, Author::on('secondary_testing')->count());

        DB::connection('secondary_testing')->flushQueryLog();
        DB::connection('secondary_testing')->enableQueryLog();

        try {
            $this->assertSame(
                ['Secondary Alice', 'Secondary Bob'],
                Author::on('secondary_testing')->orderBy('id')->get()->pluck('name')->all(),
            );
            $this->assertSame(2, Author::on('secondary_testing')->count());
            $this->assertSame([], DB::connection('secondary_testing')->getQueryLog());
        } finally {
            DB::connection('secondary_testing')->disableQueryLog();
        }
    }

    public function test_secondary_model_write_invalidates_only_secondary_namespace(): void
    {
        $this->seedDifferentAuthorsOnTwoConnections();

        Author::find(1);
        $secondary = Author::on('secondary_testing')->find(1);
        $manager = $this->cacheManager();
        $defaultBefore = $manager->currentVersion(Author::class);
        $secondaryBefore = $manager->currentVersion(Author::class, 'secondary_testing');

        $secondary?->update(['name' => 'Secondary Alicia']);

        $this->assertSame($defaultBefore, $manager->currentVersion(Author::class));
        $this->assertGreaterThan(
            $secondaryBefore,
            $manager->currentVersion(Author::class, 'secondary_testing'),
        );
        $this->assertSame('Primary Alice', Author::find(1)?->name);
        $this->assertSame('Secondary Alicia', Author::on('secondary_testing')->find(1)?->name);
    }

    public function test_on_connection_query_explain_reports_cached_strategy(): void
    {
        $this->seedDifferentAuthorsOnTwoConnections();

        $this->assertSame(
            'cached',
            Author::on('secondary_testing')->whereKey(1)->explain(),
        );
    }

    public function test_use_write_pdo_direct_lookup_does_not_reuse_replica_model_payload(): void
    {
        $paths = $this->seedReplicatedAuthorConnection();

        try {
            $this->assertSame('Replica Alice', Author::on('replicated_testing')->find(1)?->name);
            $this->assertSame(
                'Primary Alice',
                Author::on('replicated_testing')->withoutCache()->useWritePdo()->find(1)?->name,
            );
            $this->assertSame(
                'Primary Alice',
                Author::on('replicated_testing')->useWritePdo()->find(1)?->name,
            );
        } finally {
            $this->cleanupReplicatedAuthorConnection($paths);
        }
    }

    public function test_use_write_pdo_normalized_query_does_not_reuse_replica_model_payload(): void
    {
        $paths = $this->seedReplicatedAuthorConnection();

        try {
            $this->assertSame(
                ['Replica Alice'],
                Author::on('replicated_testing')->orderBy('id')->get()->pluck('name')->all(),
            );
            $this->assertSame(
                ['Primary Alice'],
                Author::on('replicated_testing')->withoutCache()->useWritePdo()->orderBy('id')->get()->pluck('name')->all(),
            );
            $this->assertSame(
                ['Primary Alice'],
                Author::on('replicated_testing')->useWritePdo()->orderBy('id')->get()->pluck('name')->all(),
            );
        } finally {
            $this->cleanupReplicatedAuthorConnection($paths);
        }
    }

    public function test_through_relation_on_secondary_connection_does_not_leak_default_connection_model_cache(): void
    {
        $this->seedThroughRelationOnTwoConnections();

        $cold = Country::on('secondary_testing')->with('posts')->find(1);
        $this->assertSame('SecondaryPost', $cold->posts->first()->title);

        // Poisons the default connection's cache entry for the same Post id, at the same
        // version — a connection-resolution bug would make the warm read below return this.
        DB::table('posts')->where('id', 1)->update(['title' => 'DefaultPostOverwrite']);
        Post::find(1);
        $this->assertSame('DefaultPostOverwrite', $this->modelCacheEntry(Post::class, 1)['title'] ?? null);

        $warm = Country::on('secondary_testing')->with('posts')->find(1);

        $this->assertSame('SecondaryPost', $warm->posts->first()->title);
    }

    public function test_pivot_relation_on_secondary_connection_does_not_leak_default_connection_model_cache(): void
    {
        $this->seedPivotRelationOnTwoConnections();

        $cold = Author::on('secondary_testing')->with('tags')->find(1);
        $this->assertSame('SecondaryTag', $cold->tags->first()->name);

        // Poisons the default connection's cache entry for the same Tag id — see above.
        DB::table('tags')->where('id', 1)->update(['name' => 'DefaultTagOverwrite']);
        Tag::find(1);
        $this->assertSame('DefaultTagOverwrite', $this->modelCacheEntry(Tag::class, 1)['name'] ?? null);

        DB::connection('secondary_testing')->flushQueryLog();
        DB::connection('secondary_testing')->enableQueryLog();

        try {
            $warm = Author::on('secondary_testing')->with('tags')->find(1);
            // A write/read key mismatch falls through to a live query but still returns
            // correct data, masking the bug — so assert on cache hits, not just the result.
            $this->assertSame([], DB::connection('secondary_testing')->getQueryLog());
        } finally {
            DB::connection('secondary_testing')->disableQueryLog();
        }

        $this->assertSame('SecondaryTag', $warm->tags->first()->name);
    }

    private function seedPivotRelationOnTwoConnections(): void
    {
        $author = Author::create(['id' => 1, 'name' => 'DefaultAuthor']);
        $tag = Tag::create(['id' => 1, 'name' => 'DefaultTag']);
        $author->tags()->attach($tag->id);

        config()->set('database.connections.secondary_testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
        DB::purge('secondary_testing');

        Schema::connection('secondary_testing')->create('authors', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->foreignId('country_id')->nullable();
            $table->timestamps();
        });
        Schema::connection('secondary_testing')->create('tags', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->timestamps();
        });
        Schema::connection('secondary_testing')->create('author_tag', function (Blueprint $table) {
            $table->foreignId('author_id');
            $table->foreignId('tag_id');
            $table->string('notes')->nullable();
            $table->primary(['author_id', 'tag_id']);
        });

        DB::connection('secondary_testing')->table('authors')->insert([
            'id' => 1, 'name' => 'SecondaryAuthor', 'created_at' => now(), 'updated_at' => now(),
        ]);
        DB::connection('secondary_testing')->table('tags')->insert([
            'id' => 1, 'name' => 'SecondaryTag', 'created_at' => now(), 'updated_at' => now(),
        ]);
        DB::connection('secondary_testing')->table('author_tag')->insert([
            'author_id' => 1, 'tag_id' => 1,
        ]);
    }

    private function seedThroughRelationOnTwoConnections(): void
    {
        Country::create(['id' => 1, 'name' => 'DefaultCountry']);
        $author = Author::create(['id' => 1, 'name' => 'DefaultAuthor', 'country_id' => 1]);
        Post::create(['id' => 1, 'title' => 'DefaultPost', 'author_id' => $author->id]);

        config()->set('database.connections.secondary_testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);
        DB::purge('secondary_testing');

        Schema::connection('secondary_testing')->create('countries', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->timestamps();
        });
        Schema::connection('secondary_testing')->create('authors', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->foreignId('country_id')->nullable();
            $table->timestamps();
        });
        Schema::connection('secondary_testing')->create('posts', function (Blueprint $table) {
            $table->id();
            $table->string('title');
            $table->unsignedInteger('views')->default(0);
            $table->boolean('published')->default(false);
            $table->json('metadata')->nullable();
            $table->foreignId('author_id');
            $table->timestamps();
            $table->softDeletes();
        });

        DB::connection('secondary_testing')->table('countries')->insert([
            'id' => 1, 'name' => 'SecondaryCountry', 'created_at' => now(), 'updated_at' => now(),
        ]);
        DB::connection('secondary_testing')->table('authors')->insert([
            'id' => 1, 'name' => 'SecondaryAuthor', 'country_id' => 1, 'created_at' => now(), 'updated_at' => now(),
        ]);
        DB::connection('secondary_testing')->table('posts')->insert([
            'id' => 1, 'title' => 'SecondaryPost', 'author_id' => 1, 'created_at' => now(), 'updated_at' => now(),
        ]);
    }

    private function seedDifferentAuthorsOnTwoConnections(): void
    {
        config()->set('database.connections.secondary_testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);

        DB::purge('secondary_testing');

        Schema::connection('secondary_testing')->create('authors', function (Blueprint $table) {
            $table->id();
            $table->string('name');
            $table->foreignId('country_id')->nullable();
            $table->timestamps();
        });

        Author::create(['id' => 1, 'name' => 'Primary Alice']);

        DB::connection('secondary_testing')->table('authors')->insert([
            'id' => 1,
            'name' => 'Secondary Alice',
            'country_id' => null,
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    /** @return array{read: string, write: string} */
    private function seedReplicatedAuthorConnection(): array
    {
        $readPath = tempnam(sys_get_temp_dir(), 'normcache-read-');
        $writePath = tempnam(sys_get_temp_dir(), 'normcache-write-');

        $this->assertNotFalse($readPath);
        $this->assertNotFalse($writePath);

        config()->set('database.connections.replicated_testing', [
            'driver' => 'sqlite',
            'database' => $writePath,
            'prefix' => '',
            'read' => ['database' => $readPath],
            'write' => ['database' => $writePath],
        ]);

        DB::purge('replicated_testing');

        $connection = DB::connection('replicated_testing');
        $this->seedAuthorPdo($connection->getReadPdo(), 'Replica Alice');
        $this->seedAuthorPdo($connection->getPdo(), 'Primary Alice');
        $this->resetClassKeyCache();

        return ['read' => $readPath, 'write' => $writePath];
    }

    private function seedAuthorPdo(\PDO $pdo, string $name): void
    {
        $pdo->exec('CREATE TABLE authors (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name VARCHAR NOT NULL, country_id INTEGER NULL, created_at DATETIME NULL, updated_at DATETIME NULL)');

        $insert = $pdo->prepare('INSERT INTO authors (id, name, country_id, created_at, updated_at) VALUES (1, :name, NULL, NULL, NULL)');
        $insert->execute(['name' => $name]);
    }

    /** @param array{read: string, write: string} $paths */
    private function cleanupReplicatedAuthorConnection(array $paths): void
    {
        DB::purge('replicated_testing');

        foreach ($paths as $path) {
            if (is_file($path)) {
                unlink($path);
            }
        }
    }
}
