-- Atomic fetch for raw/dependsOn cache entries.
-- Reads all version keys, constructs the versioned blob key, and fetches.
-- On a miss, acquires the build lock.
--
-- KEYS[1..n-2] = version keys
-- KEYS[n-1]    = raw key prefix  (raw:{classKey}: or raw:{classKey}:tag:)
-- KEYS[n]      = building key prefix  (building:{classKey}:)
-- ARGV[1]      = hash
-- ARGV[2]      = building lock TTL in seconds
--
-- Returns: {'hit', seg, blob} | {'miss', seg, false} | {'building', seg, false}
local n = #KEYS
local n_ver = n - 2

local ver_keys = {}
for i = 1, n_ver do ver_keys[i] = KEYS[i] end
local vers = redis.call('MGET', unpack(ver_keys))
for i = 1, n_ver do if not vers[i] then vers[i] = '0' end end

local seg = 'v' .. vers[1]
for i = 2, n_ver do seg = seg .. ':v' .. vers[i] end

local data = redis.call('GET', KEYS[n - 1] .. seg .. ':' .. ARGV[1])
if data then
    return {'hit', seg, data}
end

local building_key = KEYS[n] .. ARGV[1]
if redis.call('SET', building_key, '1', 'NX', 'EX', tonumber(ARGV[2])) then
    return {'miss', seg, false}
end
return {'building', seg, false}
