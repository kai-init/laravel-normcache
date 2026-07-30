<?php

namespace NormCache\Support;

use BackedEnum;
use DateTimeInterface;
use NormCache\Values\TableIdentity;
use Stringable;
use UnitEnum;

final class QueryIdentity
{
    /**
     * @param  list<string>  $dependencyHashes
     * @param  list<mixed>  $bindings
     */
    public function hash(
        string $route,
        string $rootHash,
        array $dependencyHashes,
        string $sql,
        array $bindings,
        string $namespace,
        string $operation,
    ): string {
        if (count($dependencyHashes) > 1) {
            $dependencyHashes = array_values(array_unique($dependencyHashes));
            sort($dependencyHashes, SORT_STRING);
        }

        $prepared = '';

        foreach ($bindings as $binding) {
            $prepared .= $this->binding($binding);
        }

        return hash('xxh128', TableIdentity::encodeFields([
            'nc-query',
            $route,
            $rootHash,
            TableIdentity::encodeFields($dependencyHashes),
            $sql,
            $prepared,
            $namespace,
            $operation,
        ]));
    }

    public function namespace(?string $tag): string
    {
        if ($tag === null) {
            return 'u';
        }

        return 'g' . $this->tagHash($tag);
    }

    public function tagHash(string $tag): string
    {
        if ($tag === '' || strlen($tag) > 128 || !mb_check_encoding($tag, 'UTF-8')) {
            throw new \InvalidArgumentException(
                'NormCache tag must be non-empty valid UTF-8 and at most 128 bytes.'
            );
        }

        return hash('xxh128', TableIdentity::encodeFields(['nc-tag', $tag]));
    }

    /** @param list<string> $tokens */
    public function repairHash(string $tableHash, string $generation, array $tokens): string
    {
        $tokens = array_values(array_unique($tokens));
        sort($tokens, SORT_STRING);

        return hash('xxh128', TableIdentity::encodeFields([
            'nc-repair',
            $tableHash,
            $generation,
            TableIdentity::encodeFields($tokens),
        ]));
    }

    private function binding(mixed $value): string
    {
        if ($value instanceof BackedEnum) {
            $value = $value->value;
        } elseif ($value instanceof UnitEnum) {
            $value = $value->name;
        } elseif ($value instanceof DateTimeInterface) {
            $value = $value->format('Y-m-d H:i:s.uP');
        } elseif ($value instanceof Stringable) {
            $value = (string) $value;
        }

        return match (true) {
            $value === null => '4:null0:',
            is_bool($value) => $value ? '4:bool1:1' : '4:bool1:0',
            is_int($value) => '3:int' . strlen($digits = (string) $value) . ':' . $digits,
            is_float($value) => '5:float8:' . pack('E', $value),
            is_string($value) => '6:string' . strlen($value) . ':' . $value,
            default => throw new \InvalidArgumentException('NormCache cannot hash an unsupported query binding.'),
        };
    }
}
