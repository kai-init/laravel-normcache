-- Resolve one table's state and return the membership it currently points at.
--
-- Derives ver/gen so no prior read is needed to build the membership key. Rows are
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
local membership_key = KEYS[3] .. ':m:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local membership = redis.call('GET', membership_key)

if not membership then
    return {'miss', version, generation}
end

return {'hit', version, generation, membership}
