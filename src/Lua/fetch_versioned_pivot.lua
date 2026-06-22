-- Resolve the version segment for a pivot cache lookup. Doesn't fetch the pivot payloads —
-- PHP fetches those separately via a plain MGET, much faster than Lua's bulk reply marshaling.
--
-- KEYS[1..n]    = version keys (parent, related, ...)
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- ARGV[1]       = current timestamp in ms
--
-- Returns: seg
local n = #KEYS / 2
local now = tonumber(ARGV[1])

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

return seg
