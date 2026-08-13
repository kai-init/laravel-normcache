-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = row key prefix ending in ":r:g"
-- KEYS[4] = change record key prefix ending in ":chg:"
-- ARGV[1] = mode: version | precise | generation
-- ARGV[2] = change record payload, empty when the write records nothing
-- ARGV[3] = change record TTL
-- ARGV[4..] = PK tokens for precise mode

local mode = ARGV[1]
local version = redis.call('INCR', KEYS[1])

if ARGV[2] ~= '' then
    redis.call('SETEX', KEYS[4] .. version, ARGV[3], ARGV[2])
end

if mode == 'generation' then
    local generation = redis.call('INCR', KEYS[2])
    return {version, generation}
end

if mode == 'precise' then
    local generation = redis.call('GET', KEYS[2]) or '0'

    for i = 4, #ARGV do
        redis.call('DEL', KEYS[3] .. generation .. ':' .. ARGV[i])
    end
end

return {version}
