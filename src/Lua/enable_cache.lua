-- KEYS[1] = epoch key
-- KEYS[2] = runtime disabled flag key
--
-- Advance epoch before clearing the flag so pre-disable payloads stay unreachable.

local epoch = redis.call('INCR', KEYS[1])
redis.call('DEL', KEYS[2])

return epoch
