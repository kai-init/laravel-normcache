-- Publish an all-or-nothing missing-row repair guarded by state, row guards,
-- and a non-renewable ownership lease.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3..2+n] = guard keys
-- KEYS[3+n..2+2n] = row keys
-- KEYS[3+2n] = repair lease
-- KEYS[4+2n] = token-scoped wake list
-- ARGV[1] = row count
-- ARGV[2] = expected version
-- ARGV[3] = expected generation
-- ARGV[4] = row TTL
-- ARGV[5..4+n] = expected guards
-- ARGV[5+n..4+2n] = row payloads
-- ARGV[5+2n] = owner token
-- ARGV[6+2n] = wake token count
-- ARGV[7+2n] = wake TTL

local n = tonumber(ARGV[1])
local lease_index = 3 + (2 * n)
local wake_index = 4 + (2 * n)
local token = ARGV[5 + (2 * n)]

local function release()
    redis.call('DEL', KEYS[lease_index])
    local wake_count = tonumber(ARGV[6 + (2 * n)])
    for i = 1, wake_count do
        redis.call('LPUSH', KEYS[wake_index], '1')
    end
    redis.call('EXPIRE', KEYS[wake_index], tonumber(ARGV[7 + (2 * n)]))
end

if redis.call('GET', KEYS[lease_index]) ~= token then
    return 0
end

if (redis.call('GET', KEYS[1]) or '0') ~= ARGV[2]
    or (redis.call('GET', KEYS[2]) or '0') ~= ARGV[3] then
    release()
    return 0
end

for i = 1, n do
    if (redis.call('GET', KEYS[2 + i]) or '0') ~= ARGV[4 + i] then
        release()
        return 0
    end
end

for i = 1, n do
    redis.call('SETEX', KEYS[2 + n + i], tonumber(ARGV[4]), ARGV[4 + n + i])
end
release()
return 1
