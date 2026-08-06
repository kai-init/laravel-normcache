-- Resolve one table's state and return the current query entry's membership field.
--
-- Derives ver/gen so no prior read is needed to build the query key. Rows are
-- fetched afterwards in native MGET batches and the state is revalidated after all
-- batches complete.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
--
-- Returns {'hit'|'miss', ver, gen, membership?}

local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'
local query_key = KEYS[3] .. ':q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local membership = redis.call('HGET', query_key, 'm')

if not membership then
    return {'miss', version, generation}
end

return {'hit', version, generation, membership}
