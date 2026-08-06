<?php

namespace NormCache\Planning;

final class SqlVolatilityScanner
{
    private const VOLATILE_CALLS = '/\b(?:
        app_name|benchmark|changes|clock_timestamp|connection_id|context_info|crypt_gen_random|curdate|currval|
        curtime|current_catalog|current_database|current_request_id|current_schema|current_setting|database|
        found_rows|gen_random_bytes|gen_random_uuid|get_lock|getdate|host_name|inet_client_addr|inet_client_port|
        inet_server_addr|inet_server_port|is_free_lock|is_used_lock|last_insert_id|last_insert_rowid|lastval|newid|
        newsequentialid|nextval|now|original_login|pg_backend_pid|pg_current_xact_id|pg_sleep_for|pg_sleep_until|
        pg_sleep|rand|randomblob|random_bytes|random|release_lock|row_count|schema|session_context|setval|sleep|
        statement_timestamp|suser_sname|sysdatetime|sysdate|sysutcdatetime|timeofday|total_changes|
        transaction_timestamp|txid_current|user|utc_date|utc_time|utc_timestamp|uuid_short|uuid_generate_v[0-9]+|
        uuid|pg_(?:try_)?advisory_(?:xact_)?lock(?:_shared)?|pg_advisory_unlock(?:_shared|_all)?|
        applock_(?:mode|test)|sp_getapplock
    )\s*\(/x';

    private const VOLATILE_KEYWORDS =
        '/\b(?:current_timestamp|current_date|current_time|localtimestamp|localtime|current_user|session_user|'
        . 'system_user|current_role|current_schema|current_database|current_catalog|current_path)\b'
        . '|\bnext\s+value\s+for\b|@{1,2}[a-z_][a-z0-9_$]*/';

    private const NOW_ARGUMENT =
        '/\b(?:date|time|datetime|julianday|unixepoch|strftime)\s*\([^)]*[\'"]now[\'"]/';

    private const MEMO_LIMIT = 512;

    /** @var array<string, bool> */
    private array $memo = [];

    public function isVolatile(string $sql): bool
    {
        if (isset($this->memo[$sql])) {
            return $this->memo[$sql];
        }

        if (count($this->memo) >= self::MEMO_LIMIT) {
            $this->memo = [];
        }

        return $this->memo[$sql] = $this->scan($sql);
    }

    private function scan(string $sql): bool
    {
        $sql = strtolower($sql);

        return preg_match(self::VOLATILE_CALLS, $sql) === 1
            || preg_match(self::VOLATILE_KEYWORDS, $sql) === 1
            || preg_match(self::NOW_ARGUMENT, $sql) === 1;
    }
}
