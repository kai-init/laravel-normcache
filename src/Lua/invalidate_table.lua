-- Advance one table's dependency state and apply its row action atomically.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = row key prefix ending in ":r:g"
-- ARGV[1] = mode: version | precise | generation
-- ARGV[2..] = PK tokens for precise mode

local mode = ARGV[1]
local version = redis.call('INCR', KEYS[1])

if mode == 'generation' then
    local generation = redis.call('INCR', KEYS[2])
    return {version, generation}
end

if mode == 'precise' then
    local generation = redis.call('GET', KEYS[2]) or '0'

    for i = 2, #ARGV do
        redis.call('DEL', KEYS[3] .. generation .. ':' .. ARGV[i])
    end
end

return {version}
