-- Resolve one table's version and return the current query entry's result field.
--
-- Derives the version rather than validating a caller-supplied one, so a hit needs no
-- prior read to learn which version the query key is scoped to.
--
-- KEYS[1] = version key
-- KEYS[2] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
--
-- Returns {ver, payload} with payload absent when the key holds nothing.

local version = redis.call('GET', KEYS[1]) or '0'
local payload = redis.call('HGET', KEYS[2] .. ':q:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2], 'r')

if not payload then
    return {version}
end

return {version, payload}
