-- Fetch table-scoped query entry: result, membership, or prefer-result.
-- Reply tuple: [kind, version, generation, payload, owner, token]
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
-- ARGV[3] = mode: 'result', 'membership', or 'prefer-result'
-- ARGV[4] = optional claimant token
-- ARGV[5] = optional lease TTL

local version = redis.call('GET', KEYS[1]) or '0'
local query_key = KEYS[3] .. ':q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local mode = ARGV[3]

local function miss(generation)
    local claim_token = ARGV[4]
    local lease_ttl = tonumber(ARGV[5])
    if claim_token and claim_token ~= '' and lease_ttl and lease_ttl > 0 then
        local build_key = KEYS[3] .. ':build:q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
        local claimed = redis.call('SET', build_key, claim_token, 'NX', 'EX', lease_ttl)
        local token = claimed and claim_token or (redis.call('GET', build_key) or '')
        local owner = (claimed or token == claim_token) and 1 or 0
        return {'miss', version, generation, false, owner, token}
    end
    return {'miss', version, generation, false, 0, false}
end

if mode == 'result' or mode == 'prefer-result' then
    local result = redis.call('HGET', query_key, 'r')
    if result and result ~= '' then
        return {'result', version, false, result, 0, false}
    end
    if mode == 'result' then
        return miss(false)
    end
end

local generation = redis.call('GET', KEYS[2]) or '0'
local membership = redis.call('HGET', query_key, 'm')

if membership and membership ~= '' then
    return {'membership', version, generation, membership, 0, false}
end

return miss(generation)
