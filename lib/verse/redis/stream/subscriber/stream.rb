require_relative "./base"

module Verse
  module Redis
    module Stream
      module Subscriber
        class Stream < Base

          # Configuration for the subscriber
          Config = Struct.new(
            :max_block_time,
            :min_block_time,
            :block_time_delta,
            :max_messages_count,
            keyword_init: true
          )

          ConfigSchema = Verse::Schema.define do
            field(:max_block_time, Float).default(2.0)
            field(:min_block_time, Float).default(0.1)
            field(:block_time_delta, Float).default(0.7).rule("must be between 0 and 1") { |x| x > 0 && x <= 1 }
            field(:max_messages_count, Integer).default(10).rule("must be positive integer") { |x| x > 0 }

            transform{ |data| Config.new(data) }
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
            super(redis: redis, &block)

            @sha_scripts = {}

            channels = []
            @shards = shards

            @config = validate_config(config)
            @block_time = @config.min_block_time

            @consumer_name = consumer_name
            @consumer_id = consumer_id
          end

          def start
            super
            @thread = Thread.new{ run }
            @thread.name = "Verse Redis EM - Stream Subscriber"
          end

          def stop
            super
            @thread&.join
          end

          protected

          def validate_config(config)
            result = ConfigSchema.validate(config)

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
              argv: [@consumer_name, @consumer_id, @shards, *channels.map(&:first)]
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
            channel_and_flags = acquire_locks(channels, redis)

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
            chan_flags = channels.map(&:first).map{ |x|
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
            {} # No message, normal behavior
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
            return if channels.empty?

            while !@stopped
              begin
                # Lock as much shards as we can and get the channel list
                sharded_channels = redis { |r| lock_channel_shards(r) }

                # try to retrieve messages from the locked channels + non locked channels
                output = redis { |r| read_channels(r, [*channels.map(&:first), *sharded_channels]) }

                # if we have at least one message
                if output.any?
                  # release shards which gave nothing
                  # while we are processing the messages.
                  # keep other shards locked during processing time.
                  redis { |r| unlock_empty_shards(output.keys, r) }

                  # process the messages
                  output.each do |channel, messages|
                    messages.each do |(_, message)|
                      begin
                        process_message(channel, message)
                      rescue => e
                        # log the error but continue to process messages
                        Verse.logger.error{ e }
                      end
                    end
                  end
                end
              rescue => e
                # log the error but continue the loop
                Verse.logger.error{ e }
              ensure
                # ensure to unlock all the shards
                redis { |r| unlock_channel_shards(r) }
              end
            end
          end

        end
      end
    end
  end
end
