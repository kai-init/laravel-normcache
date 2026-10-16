-- Resolve one table's version and return the current query entry's result field.
--
-- Derives the version rather than validating a caller-supplied one, so a hit needs no
-- prior read to learn which version the query key is scoped to.
--
-- KEYS[1] = version key
-- KEYS[2] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
-- ARGV[3] = optional build lease token
-- ARGV[4] = optional build lease TTL
--
-- Returns {ver, payload} on hit, {ver, false, owner, token} on a claimed miss,
-- and {ver} on a miss when claiming is disabled.

local version = redis.call('GET', KEYS[1]) or '0'
local payload = redis.call('HGET', KEYS[2] .. ':q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2], 'r')

if not payload then
    if ARGV[3] and ARGV[3] ~= '' and tonumber(ARGV[4]) and tonumber(ARGV[4]) > 0 then
        local buildKey = KEYS[2] .. ':build:q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2]
        local claimed = redis.call('SET', buildKey, ARGV[3], 'NX', 'EX', ARGV[4])
        local token = claimed and ARGV[3] or redis.call('GET', buildKey)
        local owner = claimed or token == ARGV[3]
        return {version, false, owner and 1 or 0, token or ''}
    end

    return {version}
end

return {version, payload}
