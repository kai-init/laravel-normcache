-- Fetch aggregate cache entries in one round-trip.
-- KEYS[1]              = key prefix  (agg:{parentClassKey}:)
-- KEYS[2..2N+1]        = version key pairs per spec (related, second; N = n_specs)
-- ARGV[1]              = n_parents
-- ARGV[2..1+n_parents] = parent IDs
-- ARGV[2+n_parents]    = n_specs
-- Per spec (2 entries):
--   static_suffix  = :{col}:{fn}:{name}:{chash}
--   sec_label      = 'p', 't', or '' (empty means no second version in the suffix)
--
-- Returns: {flat_data, ver_suffixes}
-- flat_data     n_specs * n_parents values in spec-major order.
-- ver_suffixes  one version suffix string per spec (e.g. ':v5' or ':v5:p3').
local prefix = KEYS[1]
local all_vers = {}
if #KEYS > 1 then all_vers = redis.call('MGET', unpack(KEYS, 2, #KEYS)) end
for i = 1, #all_vers do if not all_vers[i] then all_vers[i] = '0' end end

local n_parents  = tonumber(ARGV[1])
local parent_ids = {}
for i = 1, n_parents do parent_ids[i] = ARGV[1 + i] end

local n_specs   = tonumber(ARGV[2 + n_parents])
local spec_base = 2 + n_parents + 1

local agg_keys    = {}
local ver_suffixes = {}

for s = 0, n_specs - 1 do
    local b         = spec_base + s * 2
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
