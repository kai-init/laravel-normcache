-- Resolve versions with cooldown, fetch versioned payload, claim build lock on miss.
-- Used for: normalized query (single and multi-dep), through-relation, and result cache.
--
-- KEYS[1..n]    = version keys
-- KEYS[n+1..2n] = scheduled keys when cooldown is enabled
-- KEYS[p]       = payload key prefix
-- KEYS[p+1]     = building key prefix
-- KEYS[p+2]     = wake prefix
-- ARGV[1]       = payload hash
-- ARGV[2]       = lock suffix (= hash for normalized query/through; sha1(tag+hash) for result)
-- ARGV[3]       = current timestamp in ms
-- ARGV[4]       = building lock TTL in seconds
-- ARGV[5]       = building lock token
-- ARGV[6]       = version key count
-- ARGV[7]       = cooldown enabled (1/0)
--
-- Returns: {'hit', seg, payload} | {'miss', seg, token} | {'building', seg}

local n = tonumber(ARGV[6]) or ((#KEYS - 3) / 2)
local has_scheduled = ARGV[7] ~= '0'
local prefix_index = has_scheduled and (2 * n + 1) or (n + 1)
local now = tonumber(ARGV[3])

local vers = {}
for i = 1, n do
    local ver = redis.call('GET', KEYS[i]) or '0'
    if has_scheduled then
        local scheduled_key = KEYS[n + i]
        local due_at = redis.call('GET', scheduled_key)
        if due_at then
            local due_at_num = tonumber(due_at)
            if due_at_num and due_at_num <= now then
                redis.call('DEL', scheduled_key)
                ver = tostring(redis.call('INCR', KEYS[i]))
            elseif not due_at_num then
                redis.call('DEL', scheduled_key)
            end
        end
    end
    vers[i] = ver
end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local data = redis.call('GET', KEYS[prefix_index] .. seg .. ':' .. ARGV[1])
if data then
    return {'hit', seg, data}
end

local building_key = KEYS[prefix_index + 1] .. seg .. ':' .. ARGV[2]
if redis.call('SET', building_key, ARGV[5], 'NX', 'EX', tonumber(ARGV[4])) then
    redis.call('DEL', KEYS[prefix_index + 2] .. ARGV[2])
    return {'miss', seg, ARGV[5]}
end

return {'building', seg}
