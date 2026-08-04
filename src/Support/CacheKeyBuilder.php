<?php

namespace NormCache\Support;

use NormCache\Values\TableIdentity;

final class CacheKeyBuilder
{
    public function __construct(
        private string $keyPrefix = '',
    ) {
        if (str_contains($keyPrefix, '{') || str_contains($keyPrefix, '}')) {
            throw new \InvalidArgumentException('NormCache key prefix must not contain Redis hash-tag braces.');
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

    public function result(
        TableIdentity $table,
        string $version,
        string $namespace,
        string $queryHash,
    ): string {
        return $this->tablePrefix($table) . ":e:v{$version}:{$namespace}:{$queryHash}";
    }

    public function resultBuild(
        TableIdentity $table,
        string $version,
        string $namespace,
        string $queryHash,
    ): string {
        return $this->tablePrefix($table) . ":build:e:v{$version}:{$namespace}:{$queryHash}";
    }

    public function row(TableIdentity $table, string $generation, string $pkToken): string
    {
        return $this->rowPrefix($table, $generation) . $pkToken;
    }

    public function rowPrefix(TableIdentity $table, string $generation): string
    {
        return $this->tablePrefix($table) . ":r:g{$generation}:";
    }

    public function rowBuild(TableIdentity $table, string $generation, string $pkToken): string
    {
        return $this->tablePrefix($table) . ":build:r:g{$generation}:{$pkToken}";
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
        return $this->keyPrefix . "{nc:x:{$queryHash}}:result:{$namespace}";
    }

    public function queryGroupBuild(string $queryHash): string
    {
        return $this->keyPrefix . "{nc:x:{$queryHash}}:build";
    }

    public function queryGroupWake(string $queryHash, string $token): string
    {
        return $this->keyPrefix . "{nc:x:{$queryHash}}:wake:{$token}";
    }

    public function tagVersion(string $tagHash): string
    {
        return $this->keyPrefix . "{nc:g:{$tagHash}}:ver";
    }

    public function epoch(): string
    {
        return $this->keyPrefix . '{ncm}:epoch';
    }

    public function disabled(): string
    {
        return $this->keyPrefix . '{ncm}:disabled';
    }

    public function schemaEpoch(): string
    {
        return $this->keyPrefix . '{ncm}:schema-epoch';
    }

    public function schema(string $connection, string $epoch): string
    {
        $hash = hash('xxh128', $connection);

        return $this->keyPrefix . "{ncm:c:{$hash}}:schema:v{$epoch}";
    }

    public function tablePrefix(TableIdentity $table): string
    {
        return $this->keyPrefix . "{nc:t:{$table->hash}}";
    }
}
