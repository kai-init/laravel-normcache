-- Fetch pivot cache entries for a set of parent IDs.
--
-- KEYS[1..n]  = version keys (parent, related, ...)
-- KEYS[n+1]   = pivot key prefix (pivot:{parentKey}:{relatedKey}:)
-- ARGV[1]     = relation name
-- ARGV[2]     = constraint hash
-- ARGV[3..]   = parent IDs
--
-- Returns: {vers, [raw_data...]}
local n = #KEYS - 1
local ver_keys = {}
for i = 1, n do ver_keys[i] = KEYS[i] end
local vers = redis.call('MGET', unpack(ver_keys))
for i = 1, n do if not vers[i] then vers[i] = '0' end end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local prefix = KEYS[n+1] .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. seg .. ':'
local pivot_keys = {}
for i = 3, #ARGV do pivot_keys[#pivot_keys + 1] = prefix .. ARGV[i] end
local data = {}
if #pivot_keys > 0 then data = redis.call('MGET', unpack(pivot_keys)) end
return {vers, data}
