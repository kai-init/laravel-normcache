-- Release a build lock only when the caller still owns it.
--
-- KEYS[1] = building lock key
-- KEYS[2] = wake key
-- ARGV[1] = building lock token (optional; empty means release unconditionally)
local token = ARGV[1] or ''

if token ~= '' and redis.call('GET', KEYS[1]) ~= token then
    return 0
end

redis.call('DEL', KEYS[1])
redis.call('LPUSH', KEYS[2], '1')
redis.call('EXPIRE', KEYS[2], 10)

return 1
