module Verse
  module Redis
    module Stream
      module Subscriber
        class Base

          attr_reader :block, :channels

          def initialize(redis:, &block)
            @redis_block = redis.is_a?(Proc) ? redis : -> (&block) { block.call(redis) }
            @block = block
            @channels = []
            @stopped = true
          end

          def redis(&block)
            @redis_block.call(&block)
          end

          def start
            @stopped = false
          end

          def stop
            @stopped = true
          end

          def listen_channel(channel, lock: false)
            raise "cannot listen to a channel while the subscriber is running" unless @stopped
            @channels << [channel, lock]
          end

          def process_message(channel, message)
            @block.call(channel, message)
          end

          alias :<< :listen_channel
        end
      end
    end
  end
end
