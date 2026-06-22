-- Fetch a versioned query result with cooldown and stale-serve support.
-- Model blobs are fetched separately via a plain MGET from PHP — much faster than Lua's
-- bulk reply marshaling for the same payload.
--
-- KEYS[1] = ver key         (ver:{classKey}:)
-- KEYS[2] = scheduled key   (scheduled:{classKey}:)
-- KEYS[3] = query prefix    (query:{classKey}:v)
-- KEYS[4] = building prefix (building:{classKey}:)
-- ARGV[1] = hash
-- ARGV[2] = current timestamp in ms
-- ARGV[3] = building lock TTL in seconds
-- ARGV[4] = stale version depth (how many old versions to try; 0 disables stale serving)
-- ARGV[5] = building lock token
--
-- Returns: {status, ver, [ids|ids_raw]}
-- On a hit, status is 'hit_raw' and the id list is an undecoded JSON string — PHP decodes
-- it and handles corrupt JSON. Stale-serving below still decodes in Lua, since it needs to
-- inspect multiple candidate versions in one round trip.
local function serve_stale(ver, hash, query_prefix, depth)
    if depth <= 0 then return nil end
    local ver_num = tonumber(ver)
    for i = 1, depth do
        local stale_ver = ver_num - i
        if stale_ver < 0 then break end
        local stale_raw = redis.call('GET', query_prefix .. tostring(stale_ver) .. ':' .. hash)
        if stale_raw then
            local ok, ids = pcall(cjson.decode, stale_raw)
            if ok and type(ids) == 'table' and #ids > 0 then
                return {'stale', ver, ids}
            end
        end
    end
    return nil
end

local now = tonumber(ARGV[2])
local due_at = redis.call('GET', KEYS[2])
local ver = redis.call('GET', KEYS[1])
if not ver then ver = '0' end
if due_at then
    local due_at_num = tonumber(due_at)
    if due_at_num and due_at_num <= now then
        redis.call('DEL', KEYS[2])
        ver = tostring(redis.call('INCR', KEYS[1]))
    elseif not due_at_num then
        redis.call('DEL', KEYS[2])
    end
end

local query_key = KEYS[3] .. ver .. ':' .. ARGV[1]
local ids_raw = redis.call('GET', query_key)
if not ids_raw then
    local building_key = KEYS[4] .. ARGV[1]
    local claimed = redis.call('SET', building_key, ARGV[5], 'NX', 'EX', tonumber(ARGV[3]))
    if claimed then return {'miss', ver, ARGV[5]} end
    return serve_stale(ver, ARGV[1], KEYS[3], tonumber(ARGV[4]) or 3) or {'building', ver}
end

return {'hit_raw', ver, ids_raw}
