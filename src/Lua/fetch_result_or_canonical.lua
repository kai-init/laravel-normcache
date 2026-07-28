-- Resolve one table's current result payload or canonical membership in one slot-local read.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = result query hash
-- ARGV[3] = canonical membership query hash
--
-- Returns:
--   {'result', ver, payload}
--   {'membership', ver, gen, membership}
--   {'miss', ver, gen}

local version = redis.call('GET', KEYS[1]) or '0'
local result_key = KEYS[3] .. ':e:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
local result = redis.call('GET', result_key)

if result then
    return {'result', version, result}
end

local generation = redis.call('GET', KEYS[2]) or '0'
local membership_key = KEYS[3] .. ':m:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[3]
local membership = redis.call('GET', membership_key)

if not membership then
    return {'miss', version, generation}
end

return {'membership', version, generation, membership}
