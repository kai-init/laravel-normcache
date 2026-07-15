-- Write model attribute entries only if the version key still matches, then release the
-- build lock. The lock is always released, even when the write is skipped.
--
-- KEYS[1]        = version key (ver:{classKey}:)
-- KEYS[2..n+1]   = model attribute keys to write
-- KEYS[n+2]      = building lock key (optional; omit to skip release)
-- KEYS[n+3]      = wake key (optional; omit when there are no waiters)
-- ARGV[1]    = expected version
-- ARGV[2]    = TTL in seconds
-- ARGV[3]    = n (number of model keys)
-- ARGV[4]    = building lock token (optional; empty means release unconditionally)
-- ARGV[5..n+4] = serialized attribute values
-- ARGV[n+5]    = wake token count (optional; defaults to 1)
local token = ARGV[4] or ''
local n = tonumber(ARGV[3])
local wake_count = tonumber(ARGV[n + 5] or '1') or 1
local has_lock = #KEYS > 1 + n
local has_wake = #KEYS > 1 + n + 1

local function release_building()
    if not has_lock then return end
    local lock = KEYS[n + 2]
    if token ~= '' and redis.call('GET', lock) ~= token then return end
    redis.call('DEL', lock)
    if has_wake then
        for i = 1, wake_count do
            redis.call('LPUSH', KEYS[n + 3], '1')
        end
        redis.call('EXPIRE', KEYS[n + 3], 10)
    end
end

if has_lock and token ~= '' and redis.call('GET', KEYS[n + 2]) ~= token then
    return 0
end

local current = redis.call('GET', KEYS[1]) or '0'
if current ~= ARGV[1] then
    release_building()
    return 0
end

local ttl = tonumber(ARGV[2])

for i = 1, n do
    redis.call('SETEX', KEYS[1 + i], ttl, ARGV[4 + i])
end

release_building()
return n
