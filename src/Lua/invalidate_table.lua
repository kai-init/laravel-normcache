-- Advance one table's dependency state and apply its row action atomically.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3..] = guard keys for precise mode
-- ARGV[1] = mode: none | precise | generation
-- ARGV[2] = guard TTL
-- ARGV[3] = row key prefix ending in ":r:g"
-- ARGV[4..] = PK tokens corresponding to guard keys

local mode = ARGV[1]
local version = redis.call('INCR', KEYS[1])

if mode == 'generation' then
    local generation = redis.call('INCR', KEYS[2])
    return {version, generation}
end

if mode == 'precise' then
    local generation = redis.call('GET', KEYS[2]) or '0'
    local ttl = tonumber(ARGV[2])

    for i = 3, #KEYS do
        redis.call('INCR', KEYS[i])
        redis.call('EXPIRE', KEYS[i], ttl)
        redis.call('DEL', ARGV[3] .. generation .. ':' .. ARGV[i + 1])
    end
end

return {version}
