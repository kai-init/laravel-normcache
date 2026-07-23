-- Resolve the current version, applying a scheduled invalidation if it is due.
-- When given a model prefix and IDs, also fetch those versioned model payloads.
--
-- KEYS[1] = ver key       (ver:{classKey}:)
-- KEYS[2] = scheduled key (scheduled:{classKey}:)
-- KEYS[3] = optional model key prefix ending in ":v"
-- ARGV[1] = current timestamp in ms
-- ARGV[2] = fetch models ("0" or "1")
-- ARGV[3..n] = model IDs when fetching models
--
-- Returns: version string | {version string, raw model payloads}
local now = tonumber(ARGV[1])
local due_at = redis.call('GET', KEYS[2])
local version

if due_at then
    local due = tonumber(due_at)
    if due and due <= now then
        redis.call('DEL', KEYS[2])
        version = tostring(redis.call('INCR', KEYS[1]))
    elseif not due then
        redis.call('DEL', KEYS[2])
    end
end

version = version or redis.call('GET', KEYS[1]) or '0'

if ARGV[2] == '0' then
    return version
end

local values = {}
local chunk_size = 500
for start = 3, #ARGV, chunk_size do
    local stop = math.min(start + chunk_size - 1, #ARGV)
    local keys = {}

    for i = start, stop do
        keys[#keys + 1] = KEYS[3] .. version .. ':' .. ARGV[i]
    end

    local chunk = redis.call('MGET', unpack(keys))
    for i = 1, #chunk do
        values[start + i - 3] = chunk[i]
    end
end

return {version, values}
