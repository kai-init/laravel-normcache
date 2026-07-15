-- Re-checks still-missing model/pivot keys and atomically claims the build lock if
-- anything is still missing. Used for both model attributes and pivot payloads.
--
-- KEYS[1..n] = model/pivot keys to re-check
-- KEYS[n+1]  = building lock key
-- KEYS[n+2]  = wake key
-- ARGV[1] = lock token
-- ARGV[2] = lock TTL in seconds
--
-- Returns: {status, lockTokenOrFalse, false, rawValues}
local n = #KEYS - 2
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

if allHit then
    return {'hit', false, false, values}
end

if redis.call('SET', KEYS[n + 1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    redis.call('DEL', KEYS[n + 2])
    return {'miss', ARGV[1], false, values}
end

return {'building', false, false, values}
