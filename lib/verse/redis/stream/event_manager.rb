require "securerandom"

module Verse
  module Redis
    module Stream
      class EventManager < Verse::Event::Manager::Base
        attr_reader :service_name, :config, :logger

        def initialize(service_name, config = nil, logger = Logger.new($stdout))
          @service_name = service_name
          @config = config
          @logger = logger
        end

        def start

        end

        def stop

        end

        def with_redis(&block)
          Verse.plugins(@config.plugin_name).with_client(&block)
        end

        # Publish an event to a specific channel.
        def publish(channel, content, headers: {}, key: nil, reply_to: nil)
          message = Message.new(self, content, headers: headers, reply_to: reply_to)

          packed_message = message.to_msgpack

          with_redis do |rd|
            # Note for later: to improve bandwidth usage, we should use a LUA script
            # to prevent to pass the message twice.

            # Publish on non-persistent channel
            rd.publish(channel, packed_message)

            # Publish on persistent channel
            partition = key && find_partition(key)

            max_len = @config.streams[channel.to_sym][:maxlen]

            channel = [channel, partition].compact.join(":")
            rd.xadd(
              channel,
              packed_message,
              approximate: true,
              maxlen: max_len,
              nomkstream: true # Do not create the stream; instead subscribers are creating it on demand.
            )
          end
        end

        # Send request to a specific channel
        # @param channel [String] The channel to send the request to
        # @param payload [Hash] The payload of the request
        # @param headers [Hash] The headers of the message (if any)
        # @param timeout [Float] The timeout of the request
        # @return Promise<Message> The response of the request
        # @raise [Verse::Error::Timeout] If the request timed out
        def request(channel, content, headers: {}, reply_to: nil, timeout: 0.5)
          reply_to ||= "REPLY_TO:#{SecureRandom.hex}"

          q = Queue.new

          with_redis do |rd|
            msgpacked =
              Message.new(self, content, headers: headers, reply_to: reply_to).to_msgpack

            rd.subscribe_with_timeout(reply_to) do |on|
              on.message do |channel, message|
                q.push(Message.from(payload))
              end
            end

            rd.publish(channel, msgpacked)
          end

          q.pop(timeout)
        end

        # Send request to multiple subscribers. Wait until timeout and
        # return an array of responses.
        # @param channel [String] The channel to send the request to
        # @param payload [Hash] The payload of the request
        # @param headers [Hash] The headers of the message (if any)
        # @param timeout [Float] The timeout of the request
        # @return Promise<[Array<Message>]> The responses of the request
        def request_all(channel, content, headers: {}, reply_to: nil, timeout: 0.5)
          reply_to ||= "REPLY_TO:#{SecureRandom.hex}"

          responses = []

          with_redis do |rd|
            msgpacked =
              Message.new(self, content, headers: headers, reply_to: reply_to).to_msgpack

            rd.subscribe_with_timeout(reply_to) do |on|
              on.message do |channel, message|
                responses << Message.from(payload)
              end
            end

            rd.publish(channel, msgpacked)
          end

          sleep timeout
          responses
        end

        # Subscribe to a specific channel in a specific mode
        # @param channel [String] The channel to subscribe to
        # @param mode [Symbol] The mode of the subscription
        # @param block [Proc] The block to execute when a message is received
        # @return [Verse::Event::Subscription] The subscription object
        def subscribe(channel, mode = Verse::Event::Manager::MODE_CONSUMER, &block)
          case mode
          when Verse::Event::Manager::MODE_BROADCAST
            subscribe_broadcast channel, &block
          when Verse::Event::Manager::MODE_CONSUMER
            subscribe_consumer channel, &block
          when Verse::Event::Manager::MODE_COMMAND
            subscribe_command channel, &block
          else
            raise ArgumentError, "mode must be :broadcast, :command, :non_persistent"
          end
        end

        def subscribe_consumer(channel, &block)
          consumer_group = Verse.service_name
          consumer_id = Verse.service_id

          with_redis do |rd|
            rd.xreadgroup(
              consumer_group,
              consumer_id,
              channel,
              '>',
              count: max_count,
              block: 15)
          end

        end

        private

        # a simple lin congruential hash with two primes and 32 bits
        # wide.
        # empiric proof of pseudo-uniform distribution:
        #
        #   1000000.times.map{ rand }.map{ |x| find_partition(x) }
        #   .group_by{ |x| x }.transform_values(&:count)
        #
        def find_partition(key)
          key.to_s.each_byte.reduce(0) do |a, e|
            (e + a * 498_975_571 + 548_897_941) & 0xffffffff
          end & 15
        end


      end
    end
  end
end
