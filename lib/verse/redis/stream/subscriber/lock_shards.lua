-- Locks the shards of a stream for a given consumer group and service id

local group_name = ARGV[1]
local service_id = ARGV[2]
local shard_count = tonumber(ARGV[3])
local stream_count = #ARGV - 3

local out = {}
local idx = 1

for s = 1, stream_count do
  local stream_name = ARGV[3 + s]
  local flag = 0

  for i = 0, shard_count - 1 do
    local key = '{VERSE:STREAM:SHARDLOCK}:' .. stream_name .. ":" .. i .. ':' .. group_name
    local value = redis.call('GET', key)

    local set_value = false

    if value == false then
      -- Not locked OR locked by the same service
      set_value = true
    else
      local is_alive = redis.call('GET', '{VERSE:STREAM:SHARDLOCK}:SERVICE_LIVENESS:' .. value)
      set_value = not is_alive
    end

    if set_value then
      redis.call('SET', key, service_id, 'EX', 3200)
      flag = bit.bor(flag, bit.lshift(1, i))
    end

  end

  out[idx] = stream_name
  out[idx + 1] = flag
  idx = idx + 2
end

return out