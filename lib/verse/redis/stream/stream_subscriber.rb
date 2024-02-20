module Verse
  module Redis
    module Stream
      class StreamSubscriber
        include MonitorMixin

        Config = Struct.new(
          :max_block_time,
          :min_block_time,
          :block_time_delta,
          :max_messages_count,
          keyword_init: true
        )

        class Config
          Schema = Verse::Schema.define do
            field(:max_block_time, Float).default(2.0)
            field(:min_block_time, Float).default(0.1)
            field(:block_time_delta, Float).default(0.7).rule("must be between 0 and 1") { |x| x > 0 && x <= 1 }
            field(:max_messages_count, Integer).default(10).rule("must be positive integer") { |x| x > 0 }

            transform{ |data|
              Config.new(data)
            }
          end
        end

        # atomic lock of shards
        LOCK_SHARDS_SCRIPT = File.read(File.join(__dir__, "lock_shards.lua").freeze)

        # atomic unlock of shards
        UNLOCK_SHARDS_SCRIPT = File.read(File.join(__dir__, "unlock_shards.lua").freeze)

        # Initialize a new subscriber in charge of
        # reading messages from multiple redis streams.
        #
        # @param config [Hash] The configuration for the subscriber
        # @param consumer_name [String] The name of the consumer
        # @param consumer_id [String] The id of the consumer
        # @param redis_block [Proc] The block to execute to get a redis connection
        # @param shards [Integer] The number of shards to use
        # @param block [Proc] The block to execute when a message is received
        def initialize(config, consumer_name:, consumer_id:, redis:, shards: 16, &block)
          @sha_scripts = {}

          @subscription_list = []
          @shards = shards
          @redis_block = redis.is_a?(Proc) ? redis : -> (&block) { block.call(redis) }

          @config = validate_config(config)
          @block_time = @config.min_block_time

          @consumer_name = consumer_name
          @consumer_id = consumer_id

          @block = block
        end

        def start
          @stopped = false
          @thread = Thread.new{ run }
          @thread.name = "Verse Redis EM - Subscriber"
        end

        def stop
          @stopped = true
          @thread&.join
        end

        def listen_channel(channel)
          raise "cannot listen to a channel while the subscriber is running" if @thread&.alive?
          @subscription_list << channel
        end

        alias :<< :listen_channel

        protected

        def validate_config(config)
          result = Config::Schema.validate(config)

          return result.value if result.success?

          raise "Invalid config for redis plugin: #{result.errors}"
        end

        def run_script(script, redis, keys: [], argv: [], retried: false)
          script_id = @sha_scripts.fetch(script.object_id) {
            @sha_scripts[script.object_id] = redis.script(:load, script)
          }

          begin
            redis.evalsha(script_id, keys: keys, argv: argv)
          rescue ::Redis::CommandError => e
            if !retried && e.message.include?("NOSCRIPT")
              @sha_scripts.delete(script.object_id)
              return run_script(script, redis, keys: keys, argv: argv, retried: true)
            end

            Verse.logger.error(e)
            raise
          end
        end

        def acquire_locks(channels, redis)
          run_script(
            LOCK_SHARDS_SCRIPT,
            redis,
            keys: ["VERSE:STREAM:SHARDLOCK"],
            argv: [@consumer_name, @consumer_id, @shards, *channels]
          )
        end

        def release_locks(channel_and_flags, redis)
          run_script(
            UNLOCK_SHARDS_SCRIPT,
            redis,
            keys: ["VERSE:STREAM:SHARDLOCK"],
            argv: [@consumer_name, @consumer_id, @shards, *channel_and_flags]
          )
        end

        def lock_channel_shards(redis)
          channel_and_flags = acquire_locks(@subscription_list, redis)

          output = []
          channel_and_flags.each_slice(2).each do |channel, flag|
            @shards.times do |shard_id|
              if (flag & (1 << shard_id)) != 0
                output << "#{channel}:#{shard_id}"
              end
            end
          end

          output
        end

        def unlock_channel_shards(redis)
          chan_flags = @subscription_list.map{ |x|
            [x, 0xffffffff]
          }.flatten

          release_locks(chan_flags, redis)
        end

        def unlock_empty_shards(message_channels, redis)
          hash = {}

          message_channels.each do |channel|
            original_channel, shard_id = channel.split(/:(?!.*:)/)
            shard_id = shard_id.to_i

            hash[original_channel] ||= 0xffffffff
            # set the flag of the shard detected to zero as we
            # want to keep the lock
            hash[original_channel] &= ~(1 << shard_id)
          end

          release_locks(hash.to_a.flatten, redis)
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

        def read_stream(redis, channels)
          redis.xreadgroup(
            @consumer_name,
            @consumer_id,
            channels,
            ['>'] * channels.size,
            count: @config.max_messages_count,
            block: @block_time * 1_000, # Time to wait for messages
            noack: true # simpler, no pending list, can cause loss of messages sometime.
          )
        rescue ::Redis::TimeoutError
          puts "error?"
          {} # No message
        rescue ::Redis::CommandError => e
          if e.message.include?("NOGROUP")
            # create stream(s), attach group
            channels.each do |channel|
              Verse.logger.debug { "Create consumer group #{@consumer_name} for #{channel}" }
              redis.xgroup(:create, channel, @consumer_name, "$", mkstream: true)
            end

            {} # return nothing for this loop...
          else
            raise
          end
        end

        def read_channels(redis, channels)
          if channels.empty?
            # do not increase the block time if we have nothing to do
            # here since the probable cause is that another service
            # has already locked onto the streams.
            sleep @block_time
            return []
          end

          output = read_stream(redis, channels)

          if output.any?
            reduce_block_time
          else
            increase_block_time
          end

          output
        end

        def run
          return if @subscription_list.empty?

          while !@stopped
            begin
              # Lock as much shards as we can and get the channel list
              sharded_channels = @redis_block.call { |redis| lock_channel_shards(redis) }

              # try to retrieve messages from the locked channels + non locked channels
              output = @redis_block.call { |redis| read_channels(redis, [*@subscription_list, *sharded_channels]) }

              # if we have at least one message
              if output.any?
                # release shards which gave nothing
                # while we are processing the messages.
                # keep other shards locked during processing time.
                @redis_block.call { |redis| unlock_empty_shards(output.keys, redis) }

                # process the messages
                output.each do |channel, messages|
                  messages.each do |(_, message)|
                    @block.call(channel, message)
                  end
                end
              end
            rescue => e
              # log the error but continue the loop
              Verse.logger.error{ e }
            ensure
              # ensure to unlock all the shards
              @redis_block.call { |redis| unlock_channel_shards(redis) }
            end
          end
        end

      end
    end
  end
end
