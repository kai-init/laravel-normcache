-- Fetch pivot cache entries for a set of parent IDs.
--
-- KEYS[1..n]    = version keys (parent, related, ...)
-- KEYS[n+1..2n] = scheduled keys (one per version key, same order)
-- KEYS[2n+1]    = pivot key prefix (pivot:{parentKey}:{relatedKey}:)
-- ARGV[1]       = relation name
-- ARGV[2]       = constraint hash
-- ARGV[3]       = current timestamp in ms
-- ARGV[4..]     = parent IDs
--
-- Returns: {seg, [raw_data...]}
local n = (#KEYS - 1) / 2
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

local prefix = KEYS[2 * n + 1] .. ARGV[1] .. ':' .. ARGV[2] .. ':' .. seg .. ':'
local pivot_keys = {}
for i = 4, #ARGV do pivot_keys[#pivot_keys + 1] = prefix .. ARGV[i] end
local data = {}
if #pivot_keys > 0 then data = redis.call('MGET', unpack(pivot_keys)) end
return {seg, data}
