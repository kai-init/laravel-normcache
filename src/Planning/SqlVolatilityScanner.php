<?php

namespace NormCache\Planning;

final class SqlVolatilityScanner
{
    private const ARGUMENT_DEPENDENT_FUNCTIONS =
        '|date|datetime|julianday|strftime|time|unixepoch|';

    private const PARENTHESIZED_SQL_TOKENS =
        '|all|and|as|by|case|else|end|from|having|join|lateral|not|on|or|order|select|then|union|values|when|where|';

    private const STABLE_FUNCTIONS =
        '|abs|array_agg|avg|bit_and|bit_or|bool_and|bool_or|cast|ceil|ceiling|char_length|coalesce|concat|concat_ws|'
        . 'count|date|datetime|dense_rank|every|exists|extract|filter|floor|greatest|group_concat|if|ifnull|in|instr|'
        . 'json_agg|json_array|json_arrayagg|json_extract|json_group_array|json_group_object|json_object|json_objectagg|'
        . 'json_query|json_unquote|json_value|julianday|least|len|length|lower|ltrim|max|min|nullif|octet_length|over|'
        . 'position|rank|replace|round|row_number|rtrim|strftime|string_agg|substr|substring|sum|time|trim|unixepoch|upper|';

    private const VOLATILE_FUNCTIONS =
        '|app_name|benchmark|changes|clock_timestamp|connection_id|context_info|crypt_gen_random|curdate|currval|'
        . 'curtime|current_catalog|current_database|current_request_id|current_schema|current_setting|database|'
        . 'found_rows|gen_random_bytes|gen_random_uuid|get_lock|getdate|host_name|inet_client_addr|inet_client_port|'
        . 'inet_server_addr|inet_server_port|is_free_lock|is_used_lock|last_insert_id|last_insert_rowid|lastval|newid|'
        . 'newsequentialid|nextval|now|original_login|pg_backend_pid|pg_current_xact_id|pg_sleep|pg_sleep_for|'
        . 'pg_sleep_until|rand|random|random_bytes|randomblob|release_lock|row_count|schema|session_context|setval|sleep|'
        . 'statement_timestamp|suser_sname|sysdate|sysdatetime|sysutcdatetime|timeofday|total_changes|transaction_timestamp|'
        . 'txid_current|user|utc_date|utc_time|utc_timestamp|uuid|uuid_short|';

    public function isVolatile(string $sql): bool
    {
        $sql = strtolower($sql);

        if (preg_match('/(?:--|#|\/\*)/', $sql) !== 0) {
            return true;
        }

        $argumentDependent = false;

        if (str_contains($sql, '(')) {
            $matched = preg_match_all(
                '/(?:\b([a-z_][a-z0-9_]*)|`([^`]+)`|"([^"]+)"|\[([^\]]+)\])\s*\(/',
                $sql,
                $functions,
                PREG_SET_ORDER | PREG_UNMATCHED_AS_NULL,
            );

            if ($matched === false) {
                return true;
            }

            foreach ($functions as $matches) {
                $function = (string) ($matches[1] ?? $matches[2] ?? $matches[3] ?? $matches[4]);

                if ($this->isVolatileFunction($function)) {
                    return true;
                }

                if (
                    $matches[1] !== null
                    && $this->listed(self::PARENTHESIZED_SQL_TOKENS, $function)
                ) {
                    continue;
                }

                if (!$this->listed(self::STABLE_FUNCTIONS, $function)) {
                    return true;
                }

                $argumentDependent = $argumentDependent
                    || $this->listed(self::ARGUMENT_DEPENDENT_FUNCTIONS, $function);
            }
        }

        if (
            $this->mayContainVolatileKeyword($sql)
            && preg_match(
                '/\b(?:current_timestamp|current_date|current_time|localtimestamp|localtime|current_user|session_user|system_user|current_role|current_schema|current_database|current_catalog|current_path)\b|\bnext\s+value\s+for\b|@{1,2}[a-z_][a-z0-9_$]*/',
                $sql,
            ) !== 0
        ) {
            return true;
        }

        return $argumentDependent && preg_match(
            '/\b(?:date|time|datetime|julianday|unixepoch|strftime)\s*\([^)]*[\'\"]now[\'\"]/',
            $sql,
        ) !== 0;
    }

    private function mayContainVolatileKeyword(string $sql): bool
    {
        return str_contains($sql, 'current')
            || str_contains($sql, 'localtime')
            || str_contains($sql, 'session_user')
            || str_contains($sql, 'system_user')
            || str_contains($sql, 'next')
            || str_contains($sql, '@');
    }

    private function isVolatileFunction(string $function): bool
    {
        return $this->listed(self::VOLATILE_FUNCTIONS, $function)
            || preg_match(
                '/^(?:uuid_generate_v[0-9]+|pg_(?:try_)?advisory_(?:xact_)?lock(?:_shared)?|pg_advisory_unlock(?:_shared|_all)?|applock_(?:mode|test)|sp_getapplock)$/',
                $function,
            ) === 1;
    }

    private function listed(string $list, string $value): bool
    {
        static $lookups = [];

        if (!isset($lookups[$list])) {
            $lookups[$list] = array_fill_keys(explode('|', trim($list, '|')), true);
        }

        return isset($lookups[$list][$value]);
    }
}
