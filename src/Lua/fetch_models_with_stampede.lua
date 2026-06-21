-- Determine model-cache hit/miss/building status for a stampede-protected fetch.
-- Does not return the model values; PHP fetches those separately via a plain
-- MGET (Lua's bulk multi-string reply marshaling is dramatically slower than a
-- native MGET for the same payload, so we don't return them here).
-- If any are missing, attempts to acquire the building lock.
-- KEYS[1..n] = model keys
-- KEYS[n+1]  = lock key
-- KEYS[n+2]  = model-class version key
-- ARGV[1]    = lock token
-- ARGV[2]    = lock ttl
--
-- Returns: {status, lockTokenOrFalse, version}
local n = #KEYS - 2
local chunk_size = 500
local all_hit = true
for start = 1, n, chunk_size do
    local stop = math.min(start + chunk_size - 1, n)
    local chunk = {}
    for i = start, stop do chunk[#chunk + 1] = KEYS[i] end
    local chunk_values = redis.call('MGET', unpack(chunk))
    for i = 1, #chunk_values do
        if not chunk_values[i] then
            all_hit = false
            break
        end
    end
    if not all_hit then break end
end

local version = redis.call('GET', KEYS[n+2]) or '0'

if all_hit then
    return {'hit', false, version}
end

if redis.call('SET', KEYS[n+1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2])) then
    return {'miss', ARGV[1], version}
end

return {'building', false, version}
