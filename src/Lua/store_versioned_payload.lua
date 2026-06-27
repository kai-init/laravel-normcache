-- Write multiple payloads only if all version keys still match their expected values.
-- Always releases the building lock, even when the write is skipped.
-- On success, signals any BRPOP waiters via the wake key.
--
-- KEYS[1..n]       = version keys
-- KEYS[n+1..n+m]   = cache keys to write
-- KEYS[n+m+1]      = building lock key (or '' to skip)
-- KEYS[n+m+2]      = wake key (or '' to skip)
-- ARGV[1]          = n (number of version keys)
-- ARGV[2]          = m (number of cache keys)
-- ARGV[3]          = TTL in seconds
-- ARGV[4..n+3]     = expected version values
-- ARGV[n+4..n+m+3] = serialized payloads
-- ARGV[n+m+4]      = building lock token (optional; empty means release unconditionally)
-- ARGV[n+m+5]      = wake token count (optional; defaults to 1)

local n = tonumber(ARGV[1])
local m = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local token = ARGV[n + m + 4] or ''
local wake_count = tonumber(ARGV[n + m + 5] or '1') or 1

local function release_building()
    if KEYS[n + m + 1] == '' then return end
    if token ~= '' and redis.call('GET', KEYS[n + m + 1]) ~= token then return end
    redis.call('DEL', KEYS[n + m + 1])
    if KEYS[n + m + 2] ~= '' then
        for i = 1, wake_count do
            redis.call('LPUSH', KEYS[n + m + 2], '1')
        end
        redis.call('EXPIRE', KEYS[n + m + 2], 10)
    end
end

if KEYS[n + m + 1] ~= '' and token ~= '' and redis.call('GET', KEYS[n + m + 1]) ~= token then
    return 0
end

for i = 1, n do
    local current = redis.call('GET', KEYS[i]) or '0'
    if current ~= ARGV[3 + i] then
        release_building()
        return 0
    end
end

for i = 1, m do
    redis.call('SETEX', KEYS[n + i], ttl, ARGV[n + 3 + i])
end

release_building()
return 1
