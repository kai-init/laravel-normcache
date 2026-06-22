-- Fetch a multi-versioned query result with cooldown support.
-- Small hits (<= ARGV[5] ids) also fetch model blobs inline, saving a round trip; larger
-- hits return ids only and let PHP MGET them separately.
--
-- KEYS[1..n]    = version keys
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- KEYS[2n+1]    = query prefix
-- KEYS[2n+2]    = building prefix
-- KEYS[2n+3]    = model prefix
-- ARGV[1]       = hash
-- ARGV[2]       = current timestamp in ms
-- ARGV[3]       = building lock TTL in seconds
-- ARGV[4]       = building lock token
-- ARGV[5]       = inline model threshold (max ids to inline-fetch models for; <= 0 disables inlining)
--
-- Returns: {status, seg, [ids_raw], [models]}
-- status is 'hit_raw' (ids only) or 'hit_inline' (4th element has models, same order as ids,
-- missing entries as false).
local n = (#KEYS - 3) / 2
local now = tonumber(ARGV[2])

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

local query_key = KEYS[2 * n + 1] .. seg .. ':' .. ARGV[1]
local ids_raw = redis.call('GET', query_key)

if not ids_raw then
    local building_key = KEYS[2 * n + 2] .. seg .. ':' .. ARGV[1]
    local claimed = redis.call('SET', building_key, ARGV[4], 'NX', 'EX', tonumber(ARGV[3]))
    if claimed then return {'miss', seg, ARGV[4]} end
    return {'building', seg}
end

local threshold = tonumber(ARGV[5]) or 0
if threshold > 0 then
    local ok, ids = pcall(cjson.decode, ids_raw)
    if ok and type(ids) == 'table' and #ids > 0 and #ids <= threshold then
        local models = {}
        for i = 1, #ids do
            models[i] = redis.call('GET', KEYS[2 * n + 3] .. tostring(ids[i]))
        end
        return {'hit_inline', seg, ids_raw, models}
    end
end

return {'hit_raw', seg, ids_raw}
