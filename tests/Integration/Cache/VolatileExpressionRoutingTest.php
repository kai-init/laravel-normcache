<?php

namespace NormCache\Tests\Integration\Cache;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Schema;
use NormCache\Events\QueryBypassed;
use NormCache\Tests\Fixtures\Models\Author;
use NormCache\Tests\Fixtures\Models\RawPost;
use NormCache\Tests\TestCase;
use NormCache\Traits\Cacheable;
use PHPUnit\Framework\Attributes\DataProvider;

class ReservedNameRecord extends Model
{
    use Cacheable;

    public $timestamps = false;

    protected $table = 'reserved_name_records';

    protected $guarded = [];
}

final class VolatileExpressionRoutingTest extends TestCase
{
    private int $postId;

    protected function setUp(): void
    {
        parent::setUp();

        $author = Author::query()->create(['name' => 'Author']);
        $this->postId = (int) RawPost::query()->toBase()->insertGetId([
            'title' => 'Before',
            'views' => 1,
            'published' => true,
            'author_id' => $author->getKey(),
            'created_at' => now(),
            'updated_at' => now(),
        ]);
    }

    /** @return array<string, array{0: \Closure}> */
    public static function volatileConstructions(): array
    {
        return [
            'whereRaw' => [fn() => RawPost::query()->toBase()->whereRaw('views < random()')],
            'selectRaw' => [fn() => RawPost::query()->toBase()->selectRaw('id, random() as r')],
            'orderByRaw' => [fn() => RawPost::query()->toBase()->orderByRaw('random()')],
            'havingRaw' => [fn() => RawPost::query()->toBase()
                ->selectRaw('author_id')
                ->groupBy('author_id')
                ->havingRaw('max(views) > random()')],
            'groupByRaw' => [fn() => RawPost::query()->toBase()
                ->selectRaw('count(*) as c')
                ->groupByRaw('views + random()')],
            'DB::raw value' => [fn() => RawPost::query()->toBase()
                ->where('views', '<', DB::raw('random()'))],
            'selectSub' => [fn() => RawPost::query()->toBase()
                ->selectSub(fn($q) => $q->selectRaw('random()'), 'r')],
            'whereIn sub' => [fn() => RawPost::query()->toBase()
                ->whereIn('views', fn($q) => $q->selectRaw('random()')->from('posts'))],
        ];
    }

    #[DataProvider('volatileConstructions')]
    public function test_every_raw_entry_point_bypasses_as_volatile(\Closure $build): void
    {
        $reasons = [];
        Event::listen(QueryBypassed::class, function (QueryBypassed $event) use (&$reasons): void {
            $reasons[] = $event->reason;
        });

        $build()->dependsOn(['posts'])->get();

        $this->assertContains(
            'volatile_expression',
            $reasons,
            'the volatile fragment must be found in the builder structure',
        );
    }

    public function test_a_volatile_source_expression_bypasses_despite_declared_dependencies(): void
    {
        $reasons = [];
        Event::listen(QueryBypassed::class, function (QueryBypassed $event) use (&$reasons): void {
            $reasons[] = $event->reason;
        });

        RawPost::query()->toBase()
            ->fromRaw('(select random() as r) x')
            ->dependsOn(['posts'])
            ->get();

        $this->assertContains('volatile_expression', $reasons);
    }

    /** @return array<string, array{0: string}> */
    public static function reservedColumnNames(): array
    {
        return [
            'current_role' => ['current_role'],
            'current_user' => ['current_user'],
            'current_date' => ['current_date'],
            'localtime' => ['localtime'],
        ];
    }

    #[DataProvider('reservedColumnNames')]
    public function test_a_column_named_after_a_volatile_function_is_still_cached(string $column): void
    {
        Schema::create('reserved_name_records', function (Blueprint $table) use ($column): void {
            $table->id();
            $table->string($column)->nullable();
        });

        try {
            ReservedNameRecord::query()->create(['id' => 1, $column => 'value']);

            $read = fn() => ReservedNameRecord::query()->where($column, 'value')->get();

            $this->assertCount(1, $read());

            DB::flushQueryLog();
            DB::enableQueryLog();
            $this->assertCount(1, $read());
            DB::disableQueryLog();

            $this->assertSame(
                [],
                DB::getQueryLog(),
                "a column named {$column} must not disable caching",
            );
        } finally {
            Schema::dropIfExists('reserved_name_records');
        }
    }
}
