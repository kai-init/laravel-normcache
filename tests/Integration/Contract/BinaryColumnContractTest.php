<?php

namespace NormCache\Tests\Integration\Contract;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use NormCache\Cache\CacheRuntime;
use NormCache\Payload\RawResultCodec;
use NormCache\Support\CacheSerializer;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use NormCache\Values\CacheConfig;
use PHPUnit\Framework\Attributes\DataProvider;

class BinaryColumnModel extends Model
{
    use Cacheable;

    protected $table = 'binary_rows';

    public $timestamps = false;
}

class BinaryColumnStatement extends \PDOStatement
{
    public function fetchAll(int $mode = \PDO::FETCH_DEFAULT, mixed ...$args): array
    {
        $rows = parent::fetchAll($mode, ...$args);

        // Exercise PDO_PGSQL's bytea stream shape on SQLite too.
        foreach ($rows as $row) {
            if ($row instanceof \stdClass && is_string($row->payload ?? null)) {
                $stream = fopen('php://memory', 'w+');
                fwrite($stream, $row->payload);
                rewind($stream);
                $row->payload = $stream;
            }
        }

        return $rows;
    }
}

final class BinaryColumnContractTest extends TestCase
{
    #[DataProvider('routesAndSerializers')]
    public function test_binary_columns_preserve_database_values_and_release_leases(string $route, string $serializer): void
    {
        if ($serializer === 'igbinary' && !extension_loaded('igbinary')) {
            $this->markTestSkipped('igbinary is not installed.');
        }

        config(['normcache.serializer' => $serializer]);
        $this->app->forgetInstance(CacheConfig::class);
        $this->app->forgetInstance(CacheSerializer::class);
        $this->app->forgetInstance(RawResultCodec::class);
        $this->app->forgetScopedInstances();
        Schema::create('binary_rows', function (Blueprint $table): void {
            $table->integer('id')->primary();
            $table->binary('payload');
        });
        $pdo = DB::connection()->getPdo();
        $sqlite = DB::connection()->getDriverName() === 'sqlite';
        $statementClass = $sqlite ? $pdo->getAttribute(\PDO::ATTR_STATEMENT_CLASS) : null;
        $bytes = "binary\0\xffpayload";

        try {
            $insert = $pdo->prepare('insert into binary_rows (id, payload) values (?, ?)');
            $insert->bindValue(1, 1, \PDO::PARAM_INT);
            $insert->bindValue(2, $bytes, \PDO::PARAM_LOB);
            $insert->execute();

            if ($sqlite) {
                $pdo->setAttribute(\PDO::ATTR_STATEMENT_CLASS, [BinaryColumnStatement::class]);
            }

            $query = fn() => match ($route) {
                'direct' => BinaryColumnModel::where('id', 1),
                'canonical' => BinaryColumnModel::orderBy('id'),
                'result' => BinaryColumnModel::select('payload'),
            };
            $native = $query()->withoutCache()->first()->payload;
            $stream = is_resource($native);
            $this->assertSame($bytes, $this->contents($native));

            for ($i = 0; $i < 2; $i++) {
                $value = $query()->first()->payload;
                $this->assertSame($stream, is_resource($value));
                $this->assertSame($bytes, $this->contents($value));
                $this->assertSame([], $this->cacheKeysMatching(':build:'));
                $this->assertTrue(app(CacheRuntime::class)->available());
            }

            if ($stream) {
                $this->assertSame([], $this->cacheKeysMatching(':r:g'));
                $this->assertSame([], $this->cacheQueryKeysWithField('m'));
                $this->assertSame([], $this->cacheQueryKeysWithField('r'));
            } else {
                $this->assertWarmCacheHit(fn() => $query()->first());
            }

            $author = Author::create(['name' => 'Cacheable']);
            Author::find($author->id);
            $this->assertWarmCacheHit(fn() => Author::find($author->id));
        } finally {
            if ($sqlite) {
                $pdo->setAttribute(\PDO::ATTR_STATEMENT_CLASS, $statementClass);
            }

            Schema::dropIfExists('binary_rows');
        }
    }

    public static function routesAndSerializers(): array
    {
        return [
            ['direct', 'php'], ['canonical', 'php'], ['result', 'php'],
            ['direct', 'igbinary'], ['canonical', 'igbinary'], ['result', 'igbinary'],
        ];
    }

    private function contents(mixed $value): string
    {
        if (!is_resource($value)) {
            return $value;
        }

        try {
            return stream_get_contents($value);
        } finally {
            fclose($value);
        }
    }
}
