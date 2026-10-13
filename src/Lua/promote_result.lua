-- Promote a deferred canonical result without extending its TTL.
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = query entry key
-- ARGV[1] = expected version
-- ARGV[2] = expected generation
-- ARGV[3] = membership observed by the reader
-- ARGV[4] = result payload; empty drops the sentinel

if (redis.call('GET', KEYS[1]) or '0') ~= ARGV[1]
    or (redis.call('GET', KEYS[2]) or '0') ~= ARGV[2]
    or redis.call('HGET', KEYS[3], 'm') ~= ARGV[3]
    or redis.call('HGET', KEYS[3], 'r') ~= ''
then
    return 0
end

if ARGV[4] == '' then
    redis.call('HDEL', KEYS[3], 'r')
else
    redis.call('HSET', KEYS[3], 'r', ARGV[4])
end

return 1
