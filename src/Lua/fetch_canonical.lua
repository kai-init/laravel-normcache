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
-- ARGV[3] = optional claimant token
-- ARGV[4] = optional lease TTL
--
-- Returns {'hit', ver, gen, membership}, {'miss', ver, gen}, or
-- {'miss', ver, gen, ownerFlag, ownerToken} after a claim attempt.

local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'
local query_key = KEYS[3] .. ':q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local membership = redis.call('HGET', query_key, 'm')

if not membership then
    if not ARGV[3] or ARGV[3] == '' then
        return {'miss', version, generation}
    end

    local build_key = KEYS[3] .. ':build:q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
    local owner = 0
    local owner_token = ''
    if redis.call('SET', build_key, ARGV[3], 'EX', ARGV[4], 'NX') then
        owner = 1
        owner_token = ARGV[3]
    else
        owner_token = redis.call('GET', build_key) or ''
        if owner_token == ARGV[3] then
            owner = 1
        end
    end
    return {'miss', version, generation, owner, owner_token}
end

return {'hit', version, generation, membership}
