-- Fetch a multi-versioned query result with cooldown support.
--
-- KEYS[1..n]    = version keys
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- KEYS[2n+1]    = query prefix
-- KEYS[2n+2]    = model prefix
-- KEYS[2n+3]    = building prefix
-- ARGV[1]       = hash
-- ARGV[2]       = current timestamp in ms
-- ARGV[3]       = building lock TTL in seconds
-- ARGV[4]       = building lock token
--
-- Returns: {status, seg, [ids, models]}
local n = (#KEYS - 3) / 2
local now = tonumber(ARGV[2])

local function mget_models(model_prefix, ids)
    local keys = {}
    for i, id in ipairs(ids) do keys[i] = model_prefix .. id end
    local results = {}
    for start = 1, #keys, 500 do
        local stop = math.min(start + 499, #keys)
        local chunk = {}
        for i = start, stop do chunk[#chunk + 1] = keys[i] end
        local values = redis.call('MGET', unpack(chunk))
        for i = 1, #values do results[#results + 1] = values[i] end
    end
    return results
end

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
    local building_key = KEYS[2 * n + 3] .. seg .. ':' .. ARGV[1]
    local claimed = redis.call('SET', building_key, ARGV[4], 'NX', 'EX', tonumber(ARGV[3]))
    if claimed then return {'miss', seg, ARGV[4]} end
    return {'building', seg}
end

local ok, ids = pcall(cjson.decode, ids_raw)
if not ok or type(ids) ~= 'table' then
    redis.call('DEL', query_key)
    return {'corrupt', seg}
end

if #ids == 0 then return {'empty', seg} end

return {'hit', seg, ids, mget_models(KEYS[2 * n + 2], ids)}
