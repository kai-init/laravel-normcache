-- Fetch a raw cache entry after the version segment has already been resolved.
-- This is used by the slotting path: dependency versions may live on different
-- slots, but the raw key and building key both live on the primary model slot.
--
-- KEYS[1] = raw key       (raw:{classKey}:tag:v1:v2:hash)
-- KEYS[2] = building key  (building:{classKey}:v1:v2:lockSuffix)
-- ARGV[1] = resolved version segment
-- ARGV[2] = building lock TTL in seconds
--
-- Returns: {'hit', seg, blob} | {'miss', seg, false} | {'building', seg, false}
local data = redis.call('GET', KEYS[1])
if data then
    return {'hit', ARGV[1], data}
end

if redis.call('SET', KEYS[2], '1', 'NX', 'EX', tonumber(ARGV[2])) then
    return {'miss', ARGV[1], false}
end

return {'building', ARGV[1], false}
