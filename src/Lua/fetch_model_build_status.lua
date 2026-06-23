-- Re-checks still-missing model keys and atomically claims the build lock if anything's still
-- missing. See ModelHydrator::fetchMissedStatus().
--
-- KEYS[1..n] = model keys to recheck
-- KEYS[n+1]  = lock key
-- KEYS[n+2]  = model-class version key
-- ARGV[1] = lock token
-- ARGV[2] = lock ttl
--
-- Returns: {status, lockTokenOrFalse, version, rawValues} — rawValues are the raw MGET results
-- in KEYS[1..n] order; the caller unserializes/hydrates them.
local n = #KEYS - 2
local values = redis.call('MGET', unpack(KEYS, 1, n))
local version = redis.call('GET', KEYS[n + 2]) or '0'

local allHit = true
for i = 1, n do
    if not values[i] then
        allHit = false
        break
    end
end

if allHit then
    return {'hit', false, version, values}
end

if redis.call('SET', KEYS[n + 1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    return {'miss', ARGV[1], version, values}
end

return {'building', false, version, values}
