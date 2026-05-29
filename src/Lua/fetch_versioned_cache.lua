-- Fetch a versioned cache entry (through-relation, count, scalar).
-- Reads all version keys, builds a compound version segment, and looks up the entry.
--
-- KEYS[1..n]  = version keys
-- KEYS[n+1]   = key prefix (everything before the version segment)
-- ARGV[1]     = hash (suffix after the version segment)
--
-- Returns: {vers, data_or_false}
local n = #KEYS - 1
local ver_keys = {}
for i = 1, n do ver_keys[i] = KEYS[i] end
local vers = redis.call('MGET', unpack(ver_keys))
for i = 1, n do if not vers[i] then vers[i] = '0' end end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local data = redis.call('GET', KEYS[n+1] .. seg .. ':' .. ARGV[1])
return {vers, data or false}
