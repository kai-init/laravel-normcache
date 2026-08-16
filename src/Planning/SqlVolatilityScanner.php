<?php

namespace NormCache\Planning;

final class SqlVolatilityScanner
{
    private const VOLATILE_CALLS = '/\b(?:
        app_name|benchmark|changes|clock_timestamp|connection_id|context_info|crypt_gen_random|curdate|currval|
        curtime|current_request_id|current_setting|database|
        found_rows|gen_random_bytes|gen_random_uuid|get_lock|getdate|getutcdate|host_name|inet_client_addr|
        inet_client_port|
        inet_server_addr|inet_server_port|is_free_lock|is_used_lock|last_insert_id|last_insert_rowid|lastval|newid|
        newsequentialid|nextval|now|original_login|pg_backend_pid|pg_current_xact_id|pg_sleep_for|pg_sleep_until|
        pg_sleep|rand|randomblob|random_bytes|random|release_lock|row_count|schema|session_context|setval|sleep|
        statement_timestamp|suser_sname|sysdatetimeoffset|sysdatetime|sysdate|sysutcdatetime|timeofday|total_changes|
        transaction_timestamp|txid_current|unix_timestamp|user|utc_date|utc_time|utc_timestamp|uuid_short|
        uuid_generate_v[0-9]+|
        uuid|pg_(?:try_)?advisory_(?:xact_)?lock(?:_shared)?|pg_advisory_unlock(?:_shared|_all)?|
        applock_(?:mode|test)|sp_getapplock
    )\s*\(/x';

    private const VOLATILE_KEYWORDS =
        '/\b(?:current_timestamp|current_date|current_time|localtimestamp|localtime|current_user|session_user|'
        . 'system_user|current_role|current_schema|current_database|current_catalog|current_path)\b'
        . '|\bnext\s+value\s+for\b|@{1,2}[a-z_][a-z0-9_$]*/';

    private const NOW_ARGUMENT =
        '/\b(?:date|time|datetime|julianday|unixepoch|strftime)\s*\([^)]*[\'"]now[\'"]/';

    // Quoted text cannot call a function or read a session variable. Backslash is
    // deliberately not an escape: SQLite and PostgreSQL end the literal at 'a\', so
    // honouring it would swallow the SQL after it and hide a volatile call. Reading
    // it as ordinary only over-consumes on MySQL, which fails closed.
    private const QUOTED_TEXT = '/\'(?:[^\']|\'\')*\'|"(?:[^"]|"")*"|`[^`]*`/s';

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
        // An unterminated quote leaves the text intact, and a PCRE limit failure
        // returns null. Both fall back to scanning the raw SQL, which fails closed.
        $unquoted = preg_replace(self::QUOTED_TEXT, ' ', $sql) ?? $sql;

        return $this->matches(self::VOLATILE_CALLS, $unquoted)
            || $this->matches(self::VOLATILE_KEYWORDS, $unquoted)
            // This one reads the literal itself, as in date('now').
            || $this->matches(self::NOW_ARGUMENT, $sql);
    }

    private function matches(string $pattern, string $sql): bool
    {
        // preg_match answers false, not 0, when PCRE gives up, so testing against
        // no-match keeps an exhausted limit on the volatile side.
        return preg_match($pattern, $sql) !== 0;
    }
}
