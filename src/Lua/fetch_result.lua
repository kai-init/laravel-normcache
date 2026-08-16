-- Derives the version so hits need no prior state read.
--
-- KEYS[1] = version key
-- KEYS[2] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
--
-- Returns {ver, payload} with payload absent when the key holds nothing.

local version = redis.call('GET', KEYS[1]) or '0'
local payload = redis.call('HGET', KEYS[2] .. ':q:' .. ARGV[1] .. ':' .. ARGV[2], 'r')

if not payload then
    return {version}
end

return {version, payload}
