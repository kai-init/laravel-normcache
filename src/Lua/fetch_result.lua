-- Resolve one table's version and return the result payload it currently points at.
--
-- Derives the version rather than validating a caller-supplied one, so a hit needs no
-- prior read to learn which version the entry key is scoped to.
--
-- KEYS[1] = version key
-- KEYS[2] = table key prefix
-- ARGV[1] = query namespace
-- ARGV[2] = query hash
--
-- Returns {ver, payload} with payload absent when the key holds nothing.

local version = redis.call('GET', KEYS[1]) or '0'
local payload = redis.call('GET', KEYS[2] .. ':e:v' .. version .. ':' .. ARGV[1] .. ':' .. ARGV[2])

if not payload then
    return {version}
end

return {version, payload}
