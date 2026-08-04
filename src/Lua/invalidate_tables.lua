-- Advance several tables' dependency states in one Redis round trip.
-- This script is used only by standalone Redis because the table keys may
-- occupy different cluster hash slots.
--
-- KEYS contains three entries per table:
--   version key, generation key, row key prefix ending in ":r:g"
-- ARGV contains, per table:
--   mode, token count, followed by the PK tokens

local argument = 1

for key = 1, #KEYS, 3 do
    local mode = ARGV[argument]
    local token_count = tonumber(ARGV[argument + 1])
    argument = argument + 2

    redis.call('INCR', KEYS[key])

    if mode == 'generation' then
        redis.call('INCR', KEYS[key + 1])
    elseif mode == 'precise' then
        local generation = redis.call('GET', KEYS[key + 1]) or '0'

        for token = 1, token_count do
            redis.call('DEL', KEYS[key + 2] .. generation .. ':' .. ARGV[argument + token - 1])
        end
    end

    argument = argument + token_count
end

return 1
