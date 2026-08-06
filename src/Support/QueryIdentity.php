<?php

namespace NormCache\Support;

use NormCache\Values\TableIdentity;

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
            if ($binding instanceof \Stringable) {
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

    public function namespace(?string $tag, ?string $context = null): string
    {
        $namespace = $tag === null
            ? 'u'
            : 'g' . $this->tagHash($tag);

        if ($context === null) {
            return $namespace;
        }

        $contextNamespace = 'c' . $this->contextHash($context);

        return $tag === null
            ? $contextNamespace
            : $namespace . ':' . $contextNamespace;
    }

    public function tagHash(string $tag): string
    {
        return $this->namedHash($tag, 'tag', 'nc-tag');
    }

    public function contextHash(string $context): string
    {
        return $this->namedHash($context, 'cache context', 'nc-context');
    }

    private function namedHash(string $value, string $name, string $domain): string
    {
        if ($value === '' || strlen($value) > 128 || !mb_check_encoding($value, 'UTF-8')) {
            throw new \InvalidArgumentException(
                "NormCache {$name} must be non-empty valid UTF-8 and at most 128 bytes."
            );
        }

        return hash('xxh128', TableIdentity::encodeFields([$domain, $value]));
    }
}
