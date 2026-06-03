-- Write a payload only if all version keys still match their expected values.
-- Pass n=0 for unversioned payloads.
-- Always releases the building lock, even when the write is skipped.
-- On success, signals any BRPOP waiters via the wake key.
--
-- KEYS[1..n]   = version keys
-- KEYS[n+1]    = cache key to write
-- KEYS[n+2]    = building lock key (or '' to skip)
-- KEYS[n+3]    = wake key (or '' to skip)
-- ARGV[1]      = n (number of version keys)
-- ARGV[2]      = TTL in seconds
-- ARGV[3..n+2] = expected version values
-- ARGV[n+3]    = payload
-- ARGV[n+4]    = building lock token (optional; empty means release unconditionally)
local n = tonumber(ARGV[1])
local token = ARGV[n+4] or ''

local function release_building()
    if KEYS[n+2] == '' then return end
    if token ~= '' and redis.call('GET', KEYS[n+2]) ~= token then return end
    redis.call('DEL', KEYS[n+2])
    if KEYS[n+3] ~= '' then
        redis.call('LPUSH', KEYS[n+3], '1')
        redis.call('EXPIRE', KEYS[n+3], 10)
    end
end

if KEYS[n+2] ~= '' and token ~= '' and redis.call('GET', KEYS[n+2]) ~= token then
    return 0
end

for i = 1, n do
    local current = redis.call('GET', KEYS[i]) or '0'
    if current ~= ARGV[2 + i] then
        release_building()
        return 0
    end
end
redis.call('SETEX', KEYS[n+1], tonumber(ARGV[2]), ARGV[n+3])
release_building()
return 1
