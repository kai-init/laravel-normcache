-- Write model attribute entries only if the version key still matches, then release the
-- build lock. The lock is always released, even when the write is skipped.
--
-- KEYS[1]    = version key (ver:{classKey}:)
-- KEYS[2]    = members set key
-- KEYS[3]    = building lock key (or '' to skip release)
-- KEYS[4]    = wake key (or '' to skip)
-- KEYS[5..]  = model attribute keys to write
-- ARGV[1]    = expected version
-- ARGV[2]    = TTL in seconds
-- ARGV[3]    = n (number of model keys)
-- ARGV[4]    = building lock token (optional; empty means release unconditionally)
-- ARGV[5..n+4] = serialized attribute values
-- ARGV[n+5]    = wake token count (optional; defaults to 1)
local token = ARGV[4] or ''
local wake_count = tonumber(ARGV[tonumber(ARGV[3]) + 5] or '1') or 1

local function release_building()
    if KEYS[3] == '' then return end
    if token ~= '' and redis.call('GET', KEYS[3]) ~= token then return end
    redis.call('DEL', KEYS[3])
    if KEYS[4] ~= '' then
        for i = 1, wake_count do
            redis.call('LPUSH', KEYS[4], '1')
        end
        redis.call('EXPIRE', KEYS[4], 10)
    end
end

if KEYS[3] ~= '' and token ~= '' and redis.call('GET', KEYS[3]) ~= token then
    return 0
end

local current = redis.call('GET', KEYS[1]) or '0'
if current ~= ARGV[1] then
    release_building()
    return 0
end

local ttl = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local members = {}

for i = 1, n do
    local key = KEYS[4 + i]
    redis.call('SETEX', key, ttl, ARGV[4 + i])
    members[i] = key
end

for start = 1, n, 500 do
    local stop = math.min(start + 499, n)
    local batch = {}

    for i = start, stop do
        batch[#batch + 1] = members[i]
    end

    redis.call('SADD', KEYS[2], unpack(batch))
end
redis.call('EXPIRE', KEYS[2], ttl)

release_building()
return n
