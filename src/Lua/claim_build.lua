-- KEYS[1] = build lease key
-- ARGV[1] = claimant token
-- ARGV[2] = lease TTL in seconds

if redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX') then
    return {1, ARGV[1]}
end

local owner = redis.call('GET', KEYS[1]) or ''

if owner == ARGV[1] then
    return {1, owner}
end

return {0, owner}
