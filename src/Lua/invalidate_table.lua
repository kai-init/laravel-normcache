-- Advance one table's dependency state and apply its row action atomically.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- ARGV[1] = mode: version | precise | generation
-- ARGV[2] = row key prefix ending in ":r:g"
-- ARGV[3..] = PK tokens for precise mode

local mode = ARGV[1]
local version = redis.call('INCR', KEYS[1])

if mode == 'generation' then
    local generation = redis.call('INCR', KEYS[2])
    return {version, generation}
end

if mode == 'precise' then
    local generation = redis.call('GET', KEYS[2]) or '0'

    for i = 3, #ARGV do
        redis.call('DEL', ARGV[2] .. generation .. ':' .. ARGV[i])
    end
end

return {version}
