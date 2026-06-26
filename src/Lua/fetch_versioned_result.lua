-- Atomic fetch for result cache entries. Resolves the version segment (applying any due
-- cooldown invalidation), fetches the payload, and claims the build lock on a miss.
--
-- KEYS[1..n]       = version keys
-- KEYS[n+1..2n]    = scheduled keys (one per version key, same order)
-- KEYS[2n+1]       = result key prefix  (result:{classKey}: or result:{classKey}:tag:)
-- KEYS[2n+2]       = building key prefix  (building:{classKey}:)
-- KEYS[2n+3]       = wake key prefix
-- ARGV[1]          = query hash (used to look up the versioned result cache entry)
-- ARGV[2]          = lock suffix (sha1 of tag+hash, used as building key suffix)
-- ARGV[3]          = building lock TTL in seconds
-- ARGV[4]          = current timestamp in ms
-- ARGV[5]          = building lock token
--
-- Returns: {'hit', seg, payload} | {'miss', seg, token} | {'building', seg}

local n = (#KEYS - 3) / 2
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

local result_prefix   = KEYS[2 * n + 1]
local building_prefix = KEYS[2 * n + 2]
local wake_prefix     = KEYS[2 * n + 3]
local suffix          = ':' .. ARGV[1]

local data = redis.call('GET', result_prefix .. seg .. suffix)
if data then
    return {'hit', seg, data}
end

local building_key = building_prefix .. seg .. ':' .. ARGV[2]
if redis.call('SET', building_key, ARGV[5], 'NX', 'EX', tonumber(ARGV[3])) then
    redis.call('DEL', wake_prefix .. ARGV[2])
    return {'miss', seg, ARGV[5]}
end

return {'building', seg}
