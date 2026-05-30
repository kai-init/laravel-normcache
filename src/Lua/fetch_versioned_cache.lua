-- Fetch a versioned cache entry (through-relation, scalar, count).
-- Reads all version keys, applies any pending cooldown invalidations, builds a
-- compound version segment, and looks up the entry.
--
-- KEYS[1..n]    = version keys
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- KEYS[2n+1]    = key prefix (everything before the version segment)
-- ARGV[1]       = hash (suffix after the version segment)
-- ARGV[2]       = current timestamp in ms
--
-- Returns: {seg, data_or_false}
local n = (#KEYS - 1) / 2
local now = tonumber(ARGV[2])

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
return {seg, data or false}
