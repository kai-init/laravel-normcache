-- Re-checks missing pivot keys and atomically claims the build lock if still missing.
-- No version key needed — pivot writes are version-gated separately at write time.
--
-- KEYS[1..n] = pivot keys to recheck
-- KEYS[n+1]  = lock key
-- KEYS[n+2]  = wake key
-- ARGV[1] = lock token
-- ARGV[2] = lock ttl
--
-- Returns: {status, lockTokenOrFalse, rawValues} — rawValues are the raw MGET results in
-- KEYS[1..n] order; the caller unserializes/hydrates them.
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
    return {'hit', false, values}
end

if redis.call('SET', KEYS[n + 1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    redis.call('DEL', KEYS[n + 2])
    return {'miss', ARGV[1], values}
end

return {'building', false, values}
