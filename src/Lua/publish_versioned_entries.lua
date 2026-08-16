-- Releases owned leases on every path; successful publication wakes waiters.
--
-- KEYS[1..n]       = version keys
-- KEYS[n+1..n+m]   = cache keys to publish
-- KEYS[n+m+1]      = build lease key (optional)
-- KEYS[n+m+2]      = wake key (optional)
-- ARGV[1]          = n (number of validation keys)
-- ARGV[2]          = m (number of cache entries)
-- ARGV[3]          = entry TTL in seconds
-- ARGV[4..n+3]     = expected validation values
-- ARGV[n+4..n+m+3] = entry hash fields; empty fields use string entries
-- ARGV[n+m+4..n+2m+3] = serialized entry payloads
-- ARGV[n+2m+4]      = build lease token; an empty token owns nothing, so a lease
--                    key present with one publishes nothing and releases nothing
-- ARGV[n+2m+5]      = wake token count (optional; defaults to 1)
-- ARGV[n+2m+6]      = wake TTL (optional; defaults to 10)

local n = tonumber(ARGV[1])
local m = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local token = ARGV[n + 2 * m + 4] or ''
local wake_count = tonumber(ARGV[n + 2 * m + 5] or '1') or 1
local wake_ttl = tonumber(ARGV[n + 2 * m + 6] or '10') or 10
local has_lease = #KEYS > n + m
local has_wake = #KEYS > n + m + 1
local wake_tokens = {}

for i = 1, wake_count do
    wake_tokens[i] = '1'
end

local function wake()
    redis.call('LPUSH', KEYS[n + m + 2], unpack(wake_tokens))
end

local function release_building()
    if not has_lease then return end
    if token == '' or redis.call('GET', KEYS[n + m + 1]) ~= token then return end
    redis.call('DEL', KEYS[n + m + 1])
    if has_wake then
        wake()
        redis.call('EXPIRE', KEYS[n + m + 2], wake_ttl)
    end
end

if has_lease and (token == '' or redis.call('GET', KEYS[n + m + 1]) ~= token) then
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
    local field = ARGV[n + 3 + i]
    local payload = ARGV[n + m + 3 + i]

    if field == '' then
        redis.call('SETEX', KEYS[n + i], ttl, payload)
    else
        redis.call('HSET', KEYS[n + i], field, payload)
        redis.call('EXPIRE', KEYS[n + i], ttl)
    end
end

release_building()
return 1
