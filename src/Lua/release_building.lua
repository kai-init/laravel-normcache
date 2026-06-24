-- Release a build lock only when the caller still owns it.
--
-- KEYS[1] = building lock key
-- KEYS[2] = wake key
-- ARGV[1] = building lock token (optional; empty means release unconditionally)
-- ARGV[2] = wake token count (optional; defaults to 1)
local token = ARGV[1] or ''
local wake_count = tonumber(ARGV[2] or '1') or 1

if token ~= '' and redis.call('GET', KEYS[1]) ~= token then
    return 0
end

redis.call('DEL', KEYS[1])
if KEYS[2] ~= '' then
    for i = 1, wake_count do
        redis.call('LPUSH', KEYS[2], '1')
    end
    redis.call('EXPIRE', KEYS[2], 10)
end

return 1
