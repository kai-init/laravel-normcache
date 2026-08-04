<?php

namespace NormCache\Support;

use NormCache\Values\TableIdentity;
use Stringable;

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
            if ($binding instanceof Stringable) {
                $binding = (string) $binding;
            }

            $prepared .= match (true) {
                $binding === null => '4:null0:',
                is_int($binding) => '3:int' . strlen($digits = (string) $binding) . ':' . $digits,
                is_float($binding) => '5:float8:' . pack('E', $binding),
                is_string($binding) => '6:string' . strlen($binding) . ':' . $binding,
                default => throw new \InvalidArgumentException('NormCache cannot hash an unsupported query binding.'),
            };
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
}
