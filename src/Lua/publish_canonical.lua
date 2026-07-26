-- Publish complete canonical rows and their membership as one guarded operation.
--
-- KEYS[1] = version key
-- KEYS[2] = generation key
-- KEYS[3] = membership key
-- KEYS[4..3+n] = row keys
-- KEYS[4+n] = build lease
-- KEYS[5+n] = token-scoped wake list
-- ARGV[1] = row count
-- ARGV[2] = expected version
-- ARGV[3] = expected generation
-- ARGV[4] = membership TTL
-- ARGV[5] = row TTL
-- ARGV[6] = membership payload
-- ARGV[7..6+n] = row payloads
-- ARGV[7+n] = owner token
-- ARGV[8+n] = wake token count
-- ARGV[9+n] = wake TTL

local n = tonumber(ARGV[1])
local lease_index = 4 + n
local wake_index = 5 + n
local token = ARGV[7 + n]

local function release()
    redis.call('DEL', KEYS[lease_index])
    local wake_count = tonumber(ARGV[8 + n])
    for i = 1, wake_count do
        redis.call('LPUSH', KEYS[wake_index], '1')
    end
    redis.call('EXPIRE', KEYS[wake_index], tonumber(ARGV[9 + n]))
end

if redis.call('GET', KEYS[lease_index]) ~= token then
    return 0
end

local version = redis.call('GET', KEYS[1]) or '0'
local generation = redis.call('GET', KEYS[2]) or '0'
if version ~= ARGV[2] or generation ~= ARGV[3] then
    release()
    return 0
end

for i = 1, n do
    redis.call('SETEX', KEYS[3 + i], tonumber(ARGV[5]), ARGV[6 + i])
end
redis.call('SETEX', KEYS[3], tonumber(ARGV[4]), ARGV[6])
release()
return 1
