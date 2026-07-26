<?php

namespace NormCache\Support;

use InvalidArgumentException;
use NormCache\Values\TableIdentity;

final class CacheKeyBuilder
{
    /** @var array<string, string> */
    private array $tablePrefixes = [];

    public function __construct(
        private string $keyPrefix = '',
    ) {
        if (str_contains($keyPrefix, '{') || str_contains($keyPrefix, '}')) {
            throw new InvalidArgumentException('NormCache key prefix must not contain Redis hash-tag braces.');
        }
    }

    public function version(TableIdentity $table): string
    {
        return $this->tablePrefix($table) . ':ver';
    }

    public function generation(TableIdentity $table): string
    {
        return $this->tablePrefix($table) . ':gen';
    }

    public function membership(
        TableIdentity $table,
        string $version,
        string $namespace,
        string $queryHash,
    ): string {
        return $this->tablePrefix($table) . ":m:v{$version}:{$namespace}:{$queryHash}";
    }

    public function membershipBuild(
        TableIdentity $table,
        string $version,
        string $namespace,
        string $queryHash,
    ): string {
        return $this->tablePrefix($table) . ":build:m:v{$version}:{$namespace}:{$queryHash}";
    }

    public function exact(
        TableIdentity $table,
        string $version,
        string $namespace,
        string $queryHash,
    ): string {
        return $this->tablePrefix($table) . ":e:v{$version}:{$namespace}:{$queryHash}";
    }

    public function exactBuild(
        TableIdentity $table,
        string $version,
        string $namespace,
        string $queryHash,
    ): string {
        return $this->tablePrefix($table) . ":build:e:v{$version}:{$namespace}:{$queryHash}";
    }

    public function row(TableIdentity $table, string $generation, string $pkToken): string
    {
        return $this->tablePrefix($table) . ":r:g{$generation}:{$pkToken}";
    }

    public function rowBuild(TableIdentity $table, string $generation, string $pkToken): string
    {
        return $this->tablePrefix($table) . ":build:r:g{$generation}:{$pkToken}";
    }

    public function guard(TableIdentity $table, string $pkToken): string
    {
        return $this->tablePrefix($table) . ":guard:{$pkToken}";
    }

    public function repairBuild(TableIdentity $table, string $batchHash): string
    {
        return $this->tablePrefix($table) . ":repair:{$batchHash}:build";
    }

    public function repairWake(TableIdentity $table, string $batchHash, string $token): string
    {
        return $this->tablePrefix($table) . ":repair:{$batchHash}:wake:{$token}";
    }

    public function wake(TableIdentity $table, string $family, string $identity, string $token): string
    {
        return $this->tablePrefix($table) . ":wake:{$family}:{$identity}:{$token}";
    }

    public function queryGroupResult(string $queryHash, string $namespace): string
    {
        return $this->keyPrefix . "{nc4:x:{$queryHash}}:result:{$namespace}";
    }

    public function queryGroupBuild(string $queryHash): string
    {
        return $this->keyPrefix . "{nc4:x:{$queryHash}}:build";
    }

    public function queryGroupWake(string $queryHash, string $token): string
    {
        return $this->keyPrefix . "{nc4:x:{$queryHash}}:wake:{$token}";
    }

    public function tagVersion(string $tagHash): string
    {
        return $this->keyPrefix . "{nc4:g:{$tagHash}}:ver";
    }

    public function epoch(): string
    {
        return $this->keyPrefix . '{nc4m}:epoch';
    }

    public function tablePrefix(TableIdentity $table): string
    {
        return $this->tablePrefixes[$table->hash] ??= $this->keyPrefix . "{nc4:t:{$table->hash}}";
    }
}
