-- Persist one schema metadata field without extending the key's lifetime.
--
-- KEYS[1] = schema metadata hash for a connection
-- ARGV[1] = field
-- ARGV[2] = value
-- ARGV[3] = ttl in seconds
--
-- The expiry is claimed once, when the hash is created. Re-arming it on every
-- write would turn the configured TTL into an idle timeout that a busy
-- application slides forward forever, and a negative discovery (a table with
-- no usable primary key) would never be re-introspected on its own. EXPIRE's
-- NX flag would do this in one command but requires Redis 7.0.

redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])

if redis.call('TTL', KEYS[1]) < 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[3])
end

return 1
