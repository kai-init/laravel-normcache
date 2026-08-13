-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = result query hash
-- ARGV[3] = canonical query hash
--
-- Returns:
--   {'result', ver, payload}
--   {'membership', ver, gen, membership}
--   {'miss', ver, gen}

local version = redis.call('GET', KEYS[1]) or '0'
local result_key = KEYS[3] .. ':q:' .. ARGV[1] .. ':' .. ARGV[2]
local result = redis.call('HGET', result_key, 'r')

if result then
    return {'result', version, result}
end

local generation = redis.call('GET', KEYS[2]) or '0'
local membership_key = KEYS[3] .. ':q:' .. ARGV[1] .. ':' .. ARGV[3]
local membership = redis.call('HGET', membership_key, 'm')

if not membership then
    return {'miss', version, generation}
end

return {'membership', version, generation, membership}
