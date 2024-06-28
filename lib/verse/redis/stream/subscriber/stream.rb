# frozen_string_literal: true

require_relative "./base"

module Verse
  module Redis
    module Stream
      module Subscriber
        # This type of subscriber is in charge of reading messages from
        # multiple redis streams.
        #
        # It uses a sharding mechanism to lock the streams related to a resource
        # and avoid multiple subscribers to read messages in unordered fashion.
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
            field(:block_time_delta, Float).default(0.7).rule("must be between 0 and 1") { |x| x.positive? && x <= 1 }
            field(:max_messages_count, Integer).default(10)

            transform { |data| Config.new(data) }
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
          def initialize(config, manager:, consumer_name:, consumer_id:, redis:, shards: 16, &block)
            super(redis:, manager:, &block)

            @sha_scripts = {}.compare_by_identity
            @shards = shards

            @config = validate_config(config)
            @block_time = @config.min_block_time

            @consumer_name = consumer_name
            @consumer_id = consumer_id
          end
          # rubocop:enable Metrics/ParameterLists

          def start
            super

            return if channels.empty?

            redis { |r| init_groups(r, channels) }

            @stream_thread = Thread.new { run }
            @stream_thread.name = "Verse Redis EM - Stream Subscriber"

            @liveness_thread = Thread.new { liveness_run }
            @liveness_thread.name = "Verse Redis EM - Stream Subscriber Liveness"
          end

          def stop
            super
            @stream_thread&.join
          end

          protected

          def liveness_run
            key = "{VERSE:STREAM:SHARDLOCK}:SERVICE_LIVENESS:#{@consumer_id}"

            until @stopped
              redis do |r|
                r.set(key, 1, ex: 30)
              end

              sleep 15
            end
          rescue StandardError => e
            # Redis error? Log it and continue
            Verse.logger.error(e)
            retry
          end

          def validate_config(config)
            return config if config.is_a?(Config)

            result = ConfigSchema.validate(config)

            return result.value if result.success?

            raise "Invalid config for redis plugin: #{result.errors}"
          end

          def run_script(script, redis, keys: [], argv: [], retried: false)
            script_id = @sha_scripts.fetch(script) do
              @sha_scripts[script] = redis.script(:load, script)
            end

            begin
              redis.evalsha(script_id, keys:, argv:)
            rescue ::Redis::CommandError => e
              if !retried && e.message.include?("NOSCRIPT")
                @sha_scripts.delete(script)
                return run_script(script, redis, keys:, argv:, retried: true)
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
            channel_and_flags = acquire_locks(channels.map(&:first), redis)

            output = []
            channel_and_flags.each_slice(2).each do |channel, flag|
              @shards.times do |shard_id|
                output << "#{channel}$#{shard_id}" if (flag & (1 << shard_id)) != 0
              end
            end

            output
          end

          def unlock_channel_shards(redis)
            chan_flags = channels.map(&:first).map do |x|
              [x, 0xffffffff]
            end.flatten

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

          def init_groups(redis, channels)
            extended_channels = channels.map(&:first).map do |c|
              [c, *(@shards.times.map { |i| "#{c}$#{i}" })]
            end.flatten

            # create stream(s), attach group
            extended_channels.each do |channel|
              redis.xgroup(:create, channel, @consumer_name, "$", mkstream: true)
              Verse.logger.info { "create consumer group #{@consumer_name} for #{channel}" }
            rescue ::Redis::CommandError => e
              # ignore if BUSYGROUP it means the group already exists
              raise unless e.message.include?("BUSYGROUP")
            end
          end

          def read_stream(redis, channels)
            redis.xreadgroup(
              @consumer_name,
              @consumer_id,
              channels,
              [">"] * channels.size,
              count: @config.max_messages_count,
              block: @block_time * 1_000, # Time to wait for messages
              noack: true # simpler, no pending list, can cause loss of messages sometime.
            )
          rescue ::Redis::TimeoutError
            {} # No message, normal behavior
          rescue ::Redis::CommandError => e
            raise unless e.message.include?("NOGROUP")

            {} # return nothing for this loop...
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

          def process_messages_from_channel(channel_messages)
            channel, messages = channel_messages

            # Remove the shard id from the channel name
            channel = channel.split(/$([^$]+)$/).first

            messages.each do |(_, message)|
              message = Message.unpack(
                self,
                message["msg"],
                channel:,
                consumer_group: @consumer_name
              )
              process_message(channel, message)
            rescue StandardError => e
              # log the error but continue to process messages
              Verse.logger.error { e }
            end
          end

          def run
            return if channels.empty?

            until @stopped
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
                  output.each(&method(:process_messages_from_channel))
                end
              rescue StandardError => e
                # log the error but continue the loop
                Verse.logger.error { e }
              ensure
                # ensure to unlock all the shards
                redis { |r| unlock_channel_shards(r) }
              end
            end
          end
          # rubocop:enable Metrics/AbcSize
        end
        # rubocop:enable Metrics/ClassLength
      end
    end
  end
end
