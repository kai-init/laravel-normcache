-- Resolve one table's current result or membership query field in one slot-local read.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
-- ARGV[3] = optional claimant token
-- ARGV[4] = optional lease TTL
--
-- Returns:
--   {'result', ver, payload}
--   {'membership', ver, gen, membership}
--   {'miss', ver, gen}
--   {'miss', ver, gen, ownerFlag, ownerToken} after a claim attempt

local version = redis.call('GET', KEYS[1]) or '0'
local result_key = KEYS[3] .. ':q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local result = redis.call('HGET', result_key, 'r')

if result and result ~= '' then
    return {'result', version, result}
end

local generation = redis.call('GET', KEYS[2]) or '0'
local membership = redis.call('HGET', result_key, 'm')

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

return {'membership', version, generation, membership}
