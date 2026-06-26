-- Re-checks still-missing model/pivot keys and atomically claims the build lock if
-- anything is still missing. Used for both model attributes (with version) and pivot
-- payloads (without version).
--
-- KEYS[1..n] = model/pivot keys to re-check
-- KEYS[n+1]  = building lock key
-- KEYS[n+2]  = version key ('' to skip — pivot case)
-- KEYS[n+3]  = wake key
-- ARGV[1] = lock token
-- ARGV[2] = lock TTL in seconds
--
-- Returns: {status, lockTokenOrFalse, version|false, rawValues}
local n = #KEYS - 3
local chunkSize = 500
local values = {}
local allHit = true

for start = 1, n, chunkSize do
    local stop = math.min(start + chunkSize - 1, n)
    local chunk = {}

    for i = start, stop do
        chunk[#chunk + 1] = KEYS[i]
    end

    local chunkValues = redis.call('MGET', unpack(chunk))

    for i = 1, #chunkValues do
        values[start + i - 1] = chunkValues[i]

        if not chunkValues[i] then
            allHit = false
        end
    end
end

local version = KEYS[n + 2] ~= '' and (redis.call('GET', KEYS[n + 2]) or '0') or false

if allHit then
    return {'hit', false, version, values}
end

if redis.call('SET', KEYS[n + 1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    redis.call('DEL', KEYS[n + 3])
    return {'miss', ARGV[1], version, values}
end

return {'building', false, version, values}
