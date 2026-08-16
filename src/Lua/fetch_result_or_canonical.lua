-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = result query hash
-- ARGV[3] = canonical query hash
--
-- Returns:
--   {'result', ver, payload, gen, membership?}
--   {'membership', ver, gen, membership}
--   {'miss', ver, gen}

local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'
local result_key = KEYS[3] .. ':q:' .. ARGV[1] .. ':' .. ARGV[2]
local membership_key = KEYS[3] .. ':q:' .. ARGV[1] .. ':' .. ARGV[3]
local result, membership

if result_key == membership_key then
    local fields = redis.call('HMGET', result_key, 'r', 'm')
    result = fields[1]
    membership = fields[2]
else
    result = redis.call('HGET', result_key, 'r')
    membership = redis.call('HGET', membership_key, 'm')
end

if result then
    if membership then
        return {'result', version, result, generation, membership}
    end

    return {'result', version, result, generation}
end

if not membership then
    return {'miss', version, generation}
end

return {'membership', version, generation, membership}
