-- Resolve the current version, applying a scheduled invalidation if it is due.
--
-- KEYS[1] = ver key       (ver:{classKey}:)
-- KEYS[2] = scheduled key (scheduled:{classKey}:)
-- ARGV[1] = current timestamp in ms
--
-- Returns: version string
local now = tonumber(ARGV[1])
local due_at = redis.call('GET', KEYS[2])
if due_at then
    local due = tonumber(due_at)
    if due and due <= now then
        redis.call('DEL', KEYS[2])
        return tostring(redis.call('INCR', KEYS[1]))
    end
    if not due then redis.call('DEL', KEYS[2]) end
end
local ver = redis.call('GET', KEYS[1])
return ver or '0'
