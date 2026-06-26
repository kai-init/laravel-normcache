-- Resolve versions with cooldown, fetch versioned payload, claim build lock on miss.
-- Used for: normalized query (single and multi-dep), through-relation, and result cache.
--
-- KEYS[1..n]    = version keys
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- KEYS[2n+1]    = payload key prefix
-- KEYS[2n+2]    = building key prefix
-- KEYS[2n+3]    = wake prefix
-- ARGV[1]       = payload hash
-- ARGV[2]       = lock suffix (= hash for normalized query/through; sha1(tag+hash) for result)
-- ARGV[3]       = current timestamp in ms
-- ARGV[4]       = building lock TTL in seconds
-- ARGV[5]       = building lock token
--
-- Returns: {'hit', seg, payload} | {'miss', seg, token} | {'building', seg}

local n = (#KEYS - 3) / 2
local now = tonumber(ARGV[3])

local vers = {}
for i = 1, n do
    local ver = redis.call('GET', KEYS[i]) or '0'
    local due_at = redis.call('GET', KEYS[n + i])
    if due_at then
        local due_at_num = tonumber(due_at)
        if due_at_num and due_at_num <= now then
            redis.call('DEL', KEYS[n + i])
            ver = tostring(redis.call('INCR', KEYS[i]))
        elseif not due_at_num then
            redis.call('DEL', KEYS[n + i])
        end
    end
    vers[i] = ver
end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local data = redis.call('GET', KEYS[2 * n + 1] .. seg .. ':' .. ARGV[1])
if data then
    return {'hit', seg, data}
end

local building_key = KEYS[2 * n + 2] .. seg .. ':' .. ARGV[2]
if redis.call('SET', building_key, ARGV[5], 'NX', 'EX', tonumber(ARGV[4])) then
    redis.call('DEL', KEYS[2 * n + 3] .. ARGV[2])
    return {'miss', seg, ARGV[5]}
end

return {'building', seg}
