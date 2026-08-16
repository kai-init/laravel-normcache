-- Rows are reachable by primary key and by other memberships, so an unguarded
-- slice could resurrect a row a concurrent write has already deleted.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = build lease key
-- KEYS[4..3+n] = row keys
-- ARGV[1] = expected version
-- ARGV[2] = expected generation
-- ARGV[3] = row TTL
-- ARGV[4] = owner token
-- ARGV[5] = lease TTL
-- ARGV[6..5+n] = row payloads

if redis.call('GET', KEYS[3]) ~= ARGV[4] then
    return 0
end

local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'

if version ~= ARGV[1] or generation ~= ARGV[2] then
    return 0
end

local ttl = tonumber(ARGV[3])

for i = 4, #KEYS do
    redis.call('SETEX', KEYS[i], ttl, ARGV[i + 2])
end

redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))

return 1
