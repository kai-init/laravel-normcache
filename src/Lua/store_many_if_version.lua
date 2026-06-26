-- Write model attribute entries only if the version key still matches, then release the
-- build lock. The lock is always released, even when the write is skipped.
--
-- KEYS[1]    = version key (ver:{classKey}:)
-- KEYS[2]    = building lock key (or '' to skip release)
-- KEYS[3]    = wake key (or '' to skip)
-- KEYS[4..]  = model attribute keys to write
-- ARGV[1]    = expected version
-- ARGV[2]    = TTL in seconds
-- ARGV[3]    = n (number of model keys)
-- ARGV[4]    = building lock token (optional; empty means release unconditionally)
-- ARGV[5..n+4] = serialized attribute values
-- ARGV[n+5]    = wake token count (optional; defaults to 1)
local token = ARGV[4] or ''
local wake_count = tonumber(ARGV[tonumber(ARGV[3]) + 5] or '1') or 1

local function release_building()
    if KEYS[2] == '' then return end
    if token ~= '' and redis.call('GET', KEYS[2]) ~= token then return end
    redis.call('DEL', KEYS[2])
    if KEYS[3] ~= '' then
        for i = 1, wake_count do
            redis.call('LPUSH', KEYS[3], '1')
        end
        redis.call('EXPIRE', KEYS[3], 10)
    end
end

if KEYS[2] ~= '' and token ~= '' and redis.call('GET', KEYS[2]) ~= token then
    return 0
end

local current = redis.call('GET', KEYS[1]) or '0'
if current ~= ARGV[1] then
    release_building()
    return 0
end

local ttl = tonumber(ARGV[2])
local n = tonumber(ARGV[3])

for i = 1, n do
    redis.call('SETEX', KEYS[3 + i], ttl, ARGV[4 + i])
end

release_building()
return n
