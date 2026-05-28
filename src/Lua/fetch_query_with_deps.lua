-- Fetch a query whose cache key embeds versions from multiple model classes.
--
-- KEYS[1..n]   = version keys for primary + sorted dep classes
-- KEYS[n+1]    = query prefix    (query:{classKey}:)
-- KEYS[n+2]    = building prefix (building:{classKey}:)
-- KEYS[n+3]    = model prefix    (model:{classKey}:)
-- ARGV[1]      = hash
-- ARGV[2]      = n (number of version keys)
-- ARGV[3]      = building lock TTL in seconds
--
-- Returns: {status, vers, [ids, models]}
local n = tonumber(ARGV[2])

local ver_keys = {}
for i = 1, n do ver_keys[i] = KEYS[i] end
local vers = redis.call('MGET', unpack(ver_keys))
for i = 1, n do if not vers[i] then vers[i] = '0' end end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local query_key = KEYS[n+1] .. seg .. ':' .. ARGV[1]
local building_key = KEYS[n+2] .. seg .. ':' .. ARGV[1]

local ids_raw = redis.call('GET', query_key)
if not ids_raw then
    local claimed = redis.call('SET', building_key, '1', 'NX', 'EX', tonumber(ARGV[3]))
    if not claimed then return {'building', vers} end
    return {'miss', vers}
end

local ok, ids = pcall(cjson.decode, ids_raw)
if not ok or type(ids) ~= 'table' then
    redis.call('DEL', query_key)
    return {'corrupt', vers}
end

if #ids == 0 then return {'empty', vers} end

local models = {}
for start = 1, #ids, 500 do
    local stop = math.min(start + 499, #ids)
    local chunk = {}
    for i = start, stop do chunk[#chunk + 1] = KEYS[n+3] .. ids[i] end
    local values = redis.call('MGET', unpack(chunk))
    for i = 1, #values do models[#models + 1] = values[i] end
end

return {'hit', vers, ids, models}
