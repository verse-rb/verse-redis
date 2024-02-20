-- Locks the shards of a stream for a given consumer group and service id

local group_name = ARGV[1]
local service_id = ARGV[2]
local shard_count = tonumber(ARGV[3])
local stream_count = #ARGV - 3

local flag = 0

local out = {}
local idx = 1

for s = 1, stream_count do
  local stream_name = ARGV[3 + s]

  for i = 0, shard_count - 1 do
    local key = '{VERSE:STREAM:SHARDLOCK}:' .. stream_name .. ":" .. i .. ':' .. group_name
    local is_set = redis.call('SET', key, service_id, 'NX', 'EX', 1800)

    if is_set then
      flag = bit.bor(flag, bit.lshift(1, i))
    end
  end

  out[idx] = stream_name
  out[idx + 1] = flag
  idx = idx + 2
end

return out