<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
use Illuminate\Database\Query\Builder as IlluminateQueryBuilder;
use Illuminate\Database\Query\JoinClause;
use NormCache\Database\QueryBuilder;
use NormCache\Values\PrimaryKeyMetadata;

final class MutationKeyExtractor
{
    /**
     * @param  array<string, mixed>|null  $assigned
     * @return list<string>|null
     */
    public function extractMutation(
        QueryBuilder $query,
        PrimaryKeyMetadata $primaryKey,
        ?array $assigned,
    ): ?array {
        $tokens = $this->extract($query, $primaryKey);

        if ($tokens === null || $assigned === null) {
            return $tokens;
        }

        foreach ($assigned as $column => $value) {
            $column = $this->columnName($column);

            if ($column !== $primaryKey->column) {
                if ($column !== null && strcasecmp($column, $primaryKey->column) === 0) {
                    return null;
                }

                continue;
            }

            if ($value instanceof Expression) {
                return null;
            }

            $newToken = $primaryKey->token($value);

            if ($newToken === null) {
                return null;
            }

            $tokens[] = $newToken;
            $tokens = array_values(array_unique($tokens));
            sort($tokens, SORT_STRING);

            return $tokens;
        }

        return $tokens;
    }

    /** @return list<string>|null */
    public function extract(
        QueryBuilder $query,
        PrimaryKeyMetadata $primaryKey,
    ): ?array {
        if (!empty($query->joins) || $this->containsOpaqueSql($query)) {
            return null;
        }

        $bound = null;

        foreach ($query->wheres as $where) {
            if (
                strtolower((string) ($where['boolean'] ?? 'and')) !== 'and'
                || strcasecmp((string) ($where['type'] ?? ''), 'raw') === 0
            ) {
                return null;
            }

            if ($this->columnName($where['column'] ?? null) !== $primaryKey->column) {
                continue;
            }

            $values = match ($where['type'] ?? null) {
                'Basic' => in_array($where['operator'] ?? null, ['=', '=='], true)
                    ? [$where['value'] ?? null]
                    : null,
                'In', 'InRaw' => $where['values'] ?? null,
                default => null,
            };

            if (!is_array($values)) {
                return null;
            }

            $currentTokens = [];

            foreach ($values as $value) {
                if ($value instanceof Expression) {
                    return null;
                }

                $token = $primaryKey->token($value);

                if ($token === null) {
                    return null;
                }

                $currentTokens[$token] = true;
            }

            $bound = $bound === null
                ? $currentTokens
                : array_intersect_key($bound, $currentTokens);
        }

        if ($bound === null) {
            return null;
        }

        $result = array_keys($bound);
        sort($result, SORT_STRING);

        return $result;
    }

    private function columnName(mixed $column): ?string
    {
        if (!is_string($column)) {
            return null;
        }

        $column = trim($column, '`"[]');

        if (str_contains($column, '.')) {
            $column = substr($column, strrpos($column, '.') + 1);
        }

        $column = trim($column, '`"[]');

        return $column;
    }

    private function containsOpaqueSql(mixed $value): bool
    {
        if ($value instanceof Expression) {
            return true;
        }

        if ($value instanceof JoinClause && $this->containsOpaqueSql($value->table)) {
            return true;
        }

        if ($value instanceof IlluminateQueryBuilder) {
            foreach ([
                $value->columns,
                $value->from,
                $value->joins,
                $value->wheres,
                $value->groups,
                $value->havings,
                $value->orders,
                $value->unions,
                $value->unionOrders,
            ] as $component) {
                if ($this->containsOpaqueSql($component)) {
                    return true;
                }
            }

            return false;
        }

        if (!is_array($value)) {
            return false;
        }

        if (
            isset($value['type'])
            && is_string($value['type'])
            && strcasecmp($value['type'], 'Raw') === 0
        ) {
            return true;
        }

        foreach ($value as $item) {
            if ($this->containsOpaqueSql($item)) {
                return true;
            }
        }

        return false;
    }
}
