-- Promote a deferred canonical result without extending its TTL.
-- KEYS[1] = generation key
-- KEYS[2] = query entry key
-- ARGV[1] = expected generation
-- ARGV[2] = result payload; empty drops the sentinel

if (redis.call('GET', KEYS[1]) or '0') ~= ARGV[1]
    or redis.call('HGET', KEYS[2], 'r') ~= ''
then
    return 0
end

if ARGV[2] == '' then
    redis.call('HDEL', KEYS[2], 'r')
else
    redis.call('HSET', KEYS[2], 'r', ARGV[2])
end

return 1
