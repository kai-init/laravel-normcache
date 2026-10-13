-- Resolve runtime state and one canonical row in a single standalone-Redis script.
-- KEYS[1] = global epoch key
-- KEYS[2] = runtime-disabled key
-- KEYS[3] = table generation key
-- KEYS[4] = table key prefix
-- ARGV[1] = primary-key token
local epoch = redis.call('GET', KEYS[1]) or '0'
local disabled = redis.call('EXISTS', KEYS[2])
local generation = redis.call('GET', KEYS[3]) or '0'
local row = redis.call('GET', KEYS[4] .. ':r:g' .. generation .. ':' .. ARGV[1])

if row then
    return {epoch, tostring(disabled), generation, row}
end

return {epoch, tostring(disabled), generation}
