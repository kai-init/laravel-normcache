-- Fetch aggregate cache entries in one round-trip.
-- KEYS[1]              = key prefix  (agg:{parentClassKey}:)
-- KEYS[2..1+2N]        = version key pairs per spec (related, second; N = n_specs)
-- KEYS[2+2N..1+4N]     = scheduled key pairs per spec (same order as version keys)
-- ARGV[1]              = n_parents
-- ARGV[2..1+n_parents] = parent IDs
-- ARGV[2+n_parents]    = n_specs
-- ARGV[3+n_parents]    = current timestamp in ms
-- Per spec (2 entries starting at ARGV[4+n_parents]):
--   static_suffix  = :{col}:{fn}:{name}:{chash}
--   sec_label      = 'p', 't', or '' (empty means no second version in the suffix)
--
-- Returns: {flat_data, ver_suffixes}
-- flat_data     n_specs * n_parents values in spec-major order.
-- ver_suffixes  one version suffix string per spec (e.g. ':v5' or ':v5:p3').
local prefix    = KEYS[1]
local n_parents = tonumber(ARGV[1])
local n_specs   = tonumber(ARGV[2 + n_parents])
local now       = tonumber(ARGV[3 + n_parents])
local spec_base = 4 + n_parents

local n_ver_keys   = 2 * n_specs
local sched_offset = 1 + n_ver_keys  -- KEYS[sched_offset + i] is the scheduled key for KEYS[1 + i]

-- Apply any pending cooldown invalidations.
-- The same model can appear in multiple specs; the `processed` guard ensures we
-- only check and potentially bump each version key once.
local processed = {}
for i = 1, n_ver_keys do
    local ver_key = KEYS[1 + i]
    local sch_key = KEYS[sched_offset + i]
    if not processed[ver_key] then
        processed[ver_key] = true
        local due_at = redis.call('GET', sch_key)
        if due_at then
            local due_at_num = tonumber(due_at)
            if due_at_num and due_at_num <= now then
                redis.call('DEL', sch_key)
                redis.call('INCR', ver_key)
            elseif not due_at_num then
                redis.call('DEL', sch_key)
            end
        end
    end
end

local all_vers = {}
if n_ver_keys > 0 then all_vers = redis.call('MGET', unpack(KEYS, 2, 1 + n_ver_keys)) end
for i = 1, n_ver_keys do if not all_vers[i] then all_vers[i] = '0' end end

local parent_ids = {}
for i = 1, n_parents do parent_ids[i] = ARGV[1 + i] end

local agg_keys    = {}
local ver_suffixes = {}

for s = 0, n_specs - 1 do
    local b          = spec_base + s * 2
    local static_sfx = ARGV[b]
    local sec_label  = ARGV[b + 1]
    local rel_ver    = all_vers[2 * s + 1]
    local sec_ver    = all_vers[2 * s + 2]

    local ver_sfx = ':v' .. rel_ver
    if sec_label ~= '' then ver_sfx = ver_sfx .. ':' .. sec_label .. sec_ver end

    ver_suffixes[s + 1] = ver_sfx

    local full_sfx = static_sfx .. ver_sfx
    for _, pid in ipairs(parent_ids) do
        agg_keys[#agg_keys + 1] = prefix .. pid .. full_sfx
    end
end

if #agg_keys == 0 then return {{}, ver_suffixes} end
return {redis.call('MGET', unpack(agg_keys)), ver_suffixes}
