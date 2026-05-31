-- Atomic fetch for raw/dependsOn cache entries.
-- Reads all version keys, applies any pending cooldown invalidations, constructs
-- the versioned blob key, and fetches. On a miss, acquires the build lock.
--
-- KEYS[1..n]       = version keys
-- KEYS[n+1..2n]    = scheduled keys (one per version key, same order)
-- KEYS[2n+1]       = raw key prefix  (raw:{classKey}: or raw:{classKey}:tag:)
-- KEYS[2n+2]       = building key prefix  (building:{classKey}:)
-- ARGV[1]          = query hash (used to look up the versioned raw cache entry)
-- ARGV[2]          = lock suffix (sha1 of tag+hash, used as building key suffix)
-- ARGV[3]          = building lock TTL in seconds
-- ARGV[4]          = current timestamp in ms
--
-- Returns: {'hit', seg, blob} | {'miss', seg, false} | {'building', seg, false}
local n = (#KEYS - 2) / 2
local now = tonumber(ARGV[4])

local vers = {}
for i = 1, n do
    local ver = redis.call('GET', KEYS[i]) or '0'
    local due_at = redis.call('GET', KEYS[n + i])
    if due_at then
        local due_at_num = tonumber(due_at)
        if due_at_num and due_at_num <= now then
            redis.call('DEL', KEYS[n + i])
            ver = tostring(redis.call('INCR', KEYS[i]))
        elseif not due_at_num then
            redis.call('DEL', KEYS[n + i])
        end
    end
    vers[i] = ver
end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local raw_prefix      = KEYS[2 * n + 1]
local building_prefix = KEYS[2 * n + 2]

local data = redis.call('GET', raw_prefix .. seg .. ':' .. ARGV[1])
if data then
    return {'hit', seg, data}
end

local building_key = building_prefix .. seg .. ':' .. ARGV[2]
if redis.call('SET', building_key, '1', 'NX', 'EX', tonumber(ARGV[3])) then
    return {'miss', seg, false}
end
return {'building', seg, false}
