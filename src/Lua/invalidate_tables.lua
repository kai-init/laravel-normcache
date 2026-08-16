-- Standalone Redis only; table keys may occupy different cluster slots.
--
-- KEYS contains four entries per table:
--   version key, generation key, row key prefix ending in ":r:g",
--   change record key prefix ending in ":chg:"
-- ARGV contains, per table:
--   mode, token count, change record payload, change record TTL,
--   followed by the PK tokens

local argument = 1
local versions = {}

for key = 1, #KEYS, 4 do
    local mode = ARGV[argument]
    local token_count = tonumber(ARGV[argument + 1])
    local record = ARGV[argument + 2]
    local record_ttl = ARGV[argument + 3]
    argument = argument + 4

    local version = redis.call('INCR', KEYS[key])
    versions[#versions + 1] = version

    if record ~= '' then
        redis.call('SETEX', KEYS[key + 3] .. version, record_ttl, record)
    end

    if mode == 'generation' then
        redis.call('INCR', KEYS[key + 1])
    elseif mode == 'precise' then
        local generation = redis.call('GET', KEYS[key + 1]) or '0'
        local batch = {}

        for token = 1, token_count do
            batch[#batch + 1] = KEYS[key + 2] .. generation .. ':' .. ARGV[argument + token - 1]

            if #batch == 100 then
                redis.call('UNLINK', unpack(batch))
                batch = {}
            end
        end

        if #batch > 0 then
            redis.call('UNLINK', unpack(batch))
        end
    end

    argument = argument + token_count
end

return versions
