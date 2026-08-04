-- Re-enable the cache after a runtime disable, atomically.
--
-- KEYS[1] = epoch key
-- KEYS[2] = runtime disabled flag key
--
-- The epoch advances BEFORE the flag clears. Writes went unobserved while the
-- cache was disabled, so anything published earlier must be unreachable before
-- any node is allowed to serve from the cache again. Clearing the flag first
-- would open a window in which another node serves pre-flush payloads.

local epoch = redis.call('INCR', KEYS[1])
redis.call('DEL', KEYS[2])

return epoch
