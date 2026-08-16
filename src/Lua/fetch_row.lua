-- Derives the generation so hits need no prior state read.
--
-- KEYS[1] = generation key
-- KEYS[2] = table key prefix
-- ARGV[1] = primary-key token
--
-- Returns {gen, row} with row absent when the key holds nothing.

local generation = redis.call('GET', KEYS[1]) or '0'
local row = redis.call('GET', KEYS[2] .. ':r:g' .. generation .. ':' .. ARGV[1])

if not row then
    return {generation}
end

return {generation, row}
