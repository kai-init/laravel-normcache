-- Claims the model-cache build lock. No MGET here — the caller already knows the keys are
-- missing via its own native MGET (see ModelHydrator::fetchMissedStatus).
--
-- KEYS[1] = lock key
-- KEYS[2] = model-class version key
-- ARGV[1] = lock token
-- ARGV[2] = lock ttl
--
-- Returns: {status, lockTokenOrFalse, version}
local version = redis.call('GET', KEYS[2]) or '0'

if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    return {'miss', ARGV[1], version}
end

return {'building', false, version}
