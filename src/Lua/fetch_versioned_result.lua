-- Atomic fetch for result cache entries. Resolves the version segment (applying any due
-- cooldown invalidation), fetches the payload, and claims the build lock on a miss.
--
-- KEYS[1..n]       = version keys
-- KEYS[n+1..2n]    = scheduled keys (one per version key, same order)
-- KEYS[2n+1]       = result key prefix  (result:{classKey}: or result:{classKey}:tag:)
-- KEYS[2n+2]       = building key prefix  (building:{classKey}:)
-- KEYS[2n+3]       = wake key prefix
-- ARGV[1]          = query hash (used to look up the versioned result cache entry)
-- ARGV[2]          = lock suffix (sha1 of tag+hash, used as building key suffix)
-- ARGV[3]          = building lock TTL in seconds
-- ARGV[4]          = current timestamp in ms
-- ARGV[5]          = building lock token
-- ARGV[6]          = stale version depth (per dependency; 0 disables stale serving)
--
-- Returns: {'hit', seg, payload} | {'stale', seg, payload} | {'miss', seg, false, token} | {'building', seg, false}

-- Steps one dependency's version back at a time, holding the rest at their current value.
local function serve_stale(vers, n, prefix, suffix, depth)
    if depth <= 0 then return nil end

    for i = 1, n do
        local ver_num = tonumber(vers[i])

        for step = 1, depth do
            local stale_ver = ver_num - step
            if stale_ver < 0 then break end

            local seg = ''
            for j = 1, n do
                local v = j == i and tostring(stale_ver) or vers[j]
                seg = seg .. (j == 1 and 'v' .. v or ':v' .. v)
            end

            local raw = redis.call('GET', prefix .. seg .. suffix)
            if raw then return raw end
        end
    end

    return nil
end

local n = (#KEYS - 3) / 2
local now = tonumber(ARGV[4])

local vers = {}
for i = 1, n do
    local ver = redis.call('GET', KEYS[i]) or '0'
    local due_at = redis.call('GET', KEYS[n + i])
    if due_at then
        local due_at_num = tonumber(due_at)
        if due_at_num and due_at_num <= now then
            redis.call('DEL', KEYS[n + i])
            ver = tostring(redis.call('INCR', KEYS[i]))
        elseif not due_at_num then
            redis.call('DEL', KEYS[n + i])
        end
    end
    vers[i] = ver
end

local seg = 'v' .. vers[1]
for i = 2, n do seg = seg .. ':v' .. vers[i] end

local result_prefix   = KEYS[2 * n + 1]
local building_prefix = KEYS[2 * n + 2]
local wake_prefix     = KEYS[2 * n + 3]
local suffix          = ':' .. ARGV[1]

local data = redis.call('GET', result_prefix .. seg .. suffix)
if data then
    return {'hit', seg, data}
end

local building_key = building_prefix .. seg .. ':' .. ARGV[2]
if redis.call('SET', building_key, ARGV[5], 'NX', 'EX', tonumber(ARGV[3])) then
    redis.call('DEL', wake_prefix .. ARGV[2])
    return {'miss', seg, false, ARGV[5]}
end

local stale_raw = serve_stale(vers, n, result_prefix, suffix, tonumber(ARGV[6]) or 3)
if stale_raw then
    return {'stale', seg, stale_raw}
end

return {'building', seg, false}
