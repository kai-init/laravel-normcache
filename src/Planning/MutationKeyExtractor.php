<?php

namespace NormCache\Planning;

use Illuminate\Contracts\Database\Query\Expression;
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
        if (!empty($query->joins)) {
            return null;
        }

        $tokens = [];
        $found = false;

        foreach ($query->wheres as $where) {
            if (
                strtolower((string) ($where['boolean'] ?? 'and')) !== 'and'
                || in_array($where['type'] ?? null, ['Nested', 'Raw', 'Exists', 'NotExists'], true)
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

            $found = true;

            foreach ($values as $value) {
                if ($value instanceof Expression) {
                    return null;
                }

                $token = $primaryKey->token($value);

                if ($token === null) {
                    return null;
                }

                $tokens[$token] = true;
            }
        }

        if (!$found) {
            return null;
        }

        $result = array_keys($tokens);
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
}
