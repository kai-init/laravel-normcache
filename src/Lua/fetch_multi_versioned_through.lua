-- Fetch a multi-versioned through query result with cooldown support.
-- Returns ids/throughKeys only; the model blobs are fetched separately via a
-- plain MGET from PHP (Lua's bulk multi-string reply marshaling is dramatically
-- slower than a native MGET for the same payload, so we don't return models here).
--
-- KEYS[1..n]    = version keys
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- KEYS[2n+1]    = query prefix
-- KEYS[2n+2]    = building prefix
-- ARGV[1]       = hash
-- ARGV[2]       = current timestamp in ms
-- ARGV[3]       = building lock TTL in seconds
-- ARGV[4]       = building lock token
--
-- Returns: {status, seg, [ids, throughKeys]}
local n = (#KEYS - 2) / 2
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
local raw_payload = redis.call('GET', query_key)

if not raw_payload then
    local building_key = KEYS[2 * n + 2] .. seg .. ':' .. ARGV[1]
    local claimed = redis.call('SET', building_key, ARGV[4], 'NX', 'EX', tonumber(ARGV[3]))
    if claimed then return {'miss', seg, ARGV[4]} end
    return {'building', seg}
end

local ok, parsed = pcall(cjson.decode, raw_payload)
if not ok or type(parsed) ~= 'table' or not parsed.i or not parsed.t then
    redis.call('DEL', query_key)
    return {'corrupt', seg}
end

local ids = parsed.i
local throughKeys = parsed.t

if #ids == 0 then return {'empty', seg} end

return {'hit', seg, ids, throughKeys}
