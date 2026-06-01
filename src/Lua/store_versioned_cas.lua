-- Write a versioned payload only if all version keys still match their expected values.
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
local n = tonumber(ARGV[1])
for i = 1, n do
    local current = redis.call('GET', KEYS[i]) or '0'
    if current ~= ARGV[2 + i] then
        if KEYS[n+2] ~= '' then redis.call('DEL', KEYS[n+2]) end
        return 0
    end
end
redis.call('SETEX', KEYS[n+1], tonumber(ARGV[2]), ARGV[n+3])
if KEYS[n+2] ~= '' then redis.call('DEL', KEYS[n+2]) end
if KEYS[n+3] ~= '' then
    redis.call('LPUSH', KEYS[n+3], '1')
    redis.call('EXPIRE', KEYS[n+3], 10)
end
return 1
