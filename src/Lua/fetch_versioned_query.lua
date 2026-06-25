-- Fetch a versioned query result with cooldown, claiming the build lock on a miss.
--
-- KEYS[1] = ver key         (ver:{classKey}:)
-- KEYS[2] = scheduled key   (scheduled:{classKey}:)
-- KEYS[3] = query prefix    (query:{classKey}:v)
-- KEYS[4] = building prefix (building:{classKey}:)
-- KEYS[5] = wake prefix     (wake:{classKey}:)
-- ARGV[1] = hash
-- ARGV[2] = current timestamp in ms
-- ARGV[3] = building lock TTL in seconds
-- ARGV[4] = building lock token
--
-- Returns: {status, ver, [ids|ids_raw]}

local now = tonumber(ARGV[2])
local due_at = redis.call('GET', KEYS[2])
local ver = redis.call('GET', KEYS[1])
if not ver then ver = '0' end
if due_at then
    local due_at_num = tonumber(due_at)
    if due_at_num and due_at_num <= now then
        redis.call('DEL', KEYS[2])
        ver = tostring(redis.call('INCR', KEYS[1]))
    elseif not due_at_num then
        redis.call('DEL', KEYS[2])
    end
end

local query_key = KEYS[3] .. ver .. ':' .. ARGV[1]
local ids_raw = redis.call('GET', query_key)
if not ids_raw then
    local building_key = KEYS[4] .. ARGV[1]
    local claimed = redis.call('SET', building_key, ARGV[4], 'NX', 'EX', tonumber(ARGV[3]))
    if claimed then
        redis.call('DEL', KEYS[5] .. ARGV[1])
        return {'miss', ver, ARGV[4]}
    end
    return {'building', ver}
end

return {'hit', ver, ids_raw}
