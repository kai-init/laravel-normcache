-- Fetch multiple model keys, returning them plus the model-class version.
-- If any are missing, attempts to acquire the building lock.
-- KEYS[1..n] = model keys
-- KEYS[n+1]  = lock key
-- KEYS[n+2]  = model-class version key
-- ARGV[1]    = lock token
-- ARGV[2]    = lock ttl
local n = #KEYS - 2
local chunk_size = 500
local values = {}
for start = 1, n, chunk_size do
    local stop = math.min(start + chunk_size - 1, n)
    local chunk = {}
    for i = start, stop do chunk[#chunk + 1] = KEYS[i] end
    local chunk_values = redis.call('MGET', unpack(chunk))
    for i = 1, #chunk_values do values[#values + 1] = chunk_values[i] end
end

local version = redis.call('GET', KEYS[n+2]) or '0'

local all_hit = true
for i=1, n do
    if not values[i] then
        all_hit = false
        break
    end
end

if all_hit then
    return {'hit', values, false, version}
end

if redis.call('SET', KEYS[n+1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    return {'miss', values, ARGV[1], version}
end

return {'building', values, false, version}
