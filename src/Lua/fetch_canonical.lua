-- Resolve one table's state and return the membership it currently points at.
--
-- Derives ver/gen so no prior read is needed to build the membership key. Rows are
-- fetched afterwards by a native MGET: pulling them into Lua costs ~5x, and
-- generation-scoped keys make the separate fetch yield the same snapshot.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
-- ARGV[3] = maximum membership bytes
-- ARGV[4] = maximum membership rows
--
-- Returns {'hit'|'miss'|'oversize'|'corrupt', ver, gen, membership?}

local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'

local membership_key = KEYS[3] .. ':m:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local membership = redis.call('GET', membership_key)

if not membership then
    return {'miss', version, generation}
end

if string.len(membership) > tonumber(ARGV[3]) then
    return {'oversize', version, generation}
end

local decoded_ok, decoded = pcall(cjson.decode, membership)
if not decoded_ok or type(decoded) ~= 'table' or decoded.f ~= 4 or type(decoded.ids) ~= 'table' then
    redis.call('DEL', membership_key)
    return {'corrupt', version, generation}
end

if #decoded.ids > tonumber(ARGV[4]) then
    return {'oversize', version, generation}
end

return {'hit', version, generation, membership}
