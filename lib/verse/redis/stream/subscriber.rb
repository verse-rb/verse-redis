module Verse
  module Redis
    module Stream
      class Subscriber
        include MonitorMixin

        Config = Struct.new(
          :max_block_time,
          :min_block_time,
          :block_time_delta,
          :max_messages_count
        )

        class Config
          Schema = Verse::Schema.define do
            field(:max_block_time, Float).default(2.0)
            field(:min_block_time, Float).default(0.1)
            field(:block_time_delta, Float).default(0.7).validate("must be between 0 and 1") { |x| x > 0 && x <= 1 }
            field(:max_messages_count, Integer).default(10).validate("must be positive integer") { |x| x > 0 }

            transform{ Config.new(**data) }
          end
        end

        LOCK_SHARDS_SCRIPT = <<-LUA
          local stream_names = ARGV[1]
          local group_name = ARGV[2]
          local service_id = ARGV[3]
          local shard_count = tonumber(ARGV[4])

          local flag = 0

          local out = {}

          for _, stream_name in ipairs(stream_names) do
            for i = 0, shard_count - 1 do
              local key = 'VERSE:STREAM:SHARDLOCK:' + stream_name + ":" + i + ':' + group_name
              local is_set = redis.set(key, service_id, 'NX', 'EX', 600)

              if is_set then
                flag = flag | (1 << i)
              end
            end

            out[stream_name] = flag
          end

          return out
        LUA

        # atomic unlock of specific shards
        UNLOCK_SHARDS_SCRIPT = <<-LUA
          local stream_names = ARGV[1]
          local group_name = ARGV[2]
          local service_id = ARGV[3]
          local shard_count = tonumber(ARGV[4])

          for stream_name, flags in ipairs(stream_names) do
            for i = 0, shard_count - 1 do
              if flags & (1 << i) != 0 then
                local key = 'VERSE:STREAM:SHARDLOCK:' + stream_name + ":" + i + ':' + group_name
                local value = redis.get(key)

                if value == service_id then
                  redis.call('DEL', key)
                end
              end
            end
          end

          return '1'
        LUA


        # @param config [Hash] The configuration for the subscriber
        # @param consumer_name [String] The name of the consumer
        # @param consumer_id [String] The id of the consumer
        # @param redis_block [Proc] The block to execute to get a redis connection
        # @param shards [Integer] The number of shards to use
        def initialize(config, consumer_name, consumer_id, redis_block, shards = 16)
          @sha_scripts = {}

          @subscription_list = []
          @shards = shards
          @redis_block = redis_block

          @config = validate_config(config)

          @consumer_name = consumer_name
          @consumer_id = consumer_id

          @block_time = config.min_block_time

          @cond = new_cond

          t = Thread.new{ run }
          t.name = "Verse Redis EM - Subscriber"
        end

        def run_script(script, redis, keys: [], argv: [])
          script_id = @sha_scripts.fetch(script.object_id) {
            redis.script(:load, script)
          }

          redis.evalsha(script_id, keys: keys, argv: argv)
        end

        def acquire_locks(channels, redis)
          run_script(
            LOCK_SCRIPT,
            redis,
            keys: [],
            argv: [channels, Verse.service_name, Verse.service_id, @shards]
          )
        end

        def release_locks(channels, redis)
          run_script(
            UNLOCK_SCRIPT,
            redis,
            keys: [],
            argv: [channels, Verse.service_name, Verse.service_id, @shards]
          )
        end

        def lock_channel_shards(redis)
          acquire_locks(@subscription_list.keys, redis)
        end

        def unlock_channel_shards(redis)
          chan_flags = @subscription_list.keys.map{ |x|
            [x, 0xffffffff]
          }.to_h

          release_locks(chan_flags, redis)
        end

        def unlock_empty_shards(message_channels)
          hash = {}

          message_channels.each do |channel|
            original_channel, shard_id = channel.split(/:(?!.*:)/)
            shard_id = shard_id.to_i

            hash[original_channel] ||= 0xffffffff
            # set the flag of the shard detected to zero as we
            # want to keep the lock
            hash[original_channel] &= ~(1 << shard_id)
          end

          hash
        end

        def reduce_block_time
          @block_time = [
            @config.block_time_delta * @block_time,
            @config.min_block_time
          ].max
        end

        def increase_block_time
          @block_time = [
            @block_time / @config.block_time_delta,
            @config.max_block_time
          ].min
        end

        def read_channels(redis, channels)
          if channels.empty?
            sleep @block_time
            return []
          end

          output = redis.xreadgroup(
            consumer_group,
            consumer_id,
            channels,
            '>',
            count: @config.max_messages_count,
            block: @block_time, # Time to wait for messages
            noack: true # simpler, no pending list, can cause loss of messages sometime.
          )

          if output.any?
            reduce_block_time
          else
            increase_block_time
          end

          output
        end

        def listen_channel(channel)
          synchronize do
            @subscription_list << true
            @cond.signal
          end
        end

        def run
          loop do
            synchronize do
              begin
                @cond.wait if @subscription_list.empty?

                @redis_block.call do |redis|
                  begin
                    # Lock as much shards as we can and get the channel list
                    channels = lock_channel_shards(redis)

                    # try to retrieve messages from the locked channels
                    output = read_channels(redis, channels)

                    # if we have at least one message
                    if output.any?
                      # release shards which gave nothing
                      # while we are processing the messages.
                      # keep other shards locked during processing time.
                      unlock_empty_shards(output.keys)

                      # process the messages
                      process_messages(output)
                    end
                  ensure
                    # ensure to unlock all the shards
                    unlock_channel_shards(redis)
                  end
                end

                output.each do |channel, messages|
                  messages.each do |message|
                    @manager.trigger(channel, message)
                  end
                end

              rescue => e
                # log the error
                Verse.logger.error{ e }
              end
            end
          end

        end

      end
    end
  end
end
