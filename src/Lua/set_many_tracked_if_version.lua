-- Write model attribute entries only if the version key still matches.
-- Skips all writes silently if the version has moved on.
--
-- KEYS[1]    = version key (ver:{classKey}:)
-- KEYS[2]    = members set key
-- KEYS[3..]  = model attribute keys to write
-- ARGV[1]    = expected version
-- ARGV[2]    = TTL in seconds
-- ARGV[3]    = n (number of model keys)
-- ARGV[4..]  = serialized attribute values
local current = redis.call('GET', KEYS[1]) or '0'
if current ~= ARGV[1] then return 0 end

local ttl = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local members = {}

for i = 1, n do
    local key = KEYS[2 + i]
    redis.call('SETEX', key, ttl, ARGV[3 + i])
    members[i] = key
end

redis.call('SADD', KEYS[2], unpack(members))
redis.call('EXPIRE', KEYS[2], ttl)

return n
