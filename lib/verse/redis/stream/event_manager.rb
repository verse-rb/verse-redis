# frozen_string_literal: true

require "securerandom"
require "monitor"

require_relative "./subscriber/stream"
require_relative "./subscriber/simple"

require_relative "./message"
require_relative "./config"

module Verse
  module Redis
    module Stream
      class EventManager < Verse::Event::Manager::Base
        Verse::Event::Manager.add_event_manager_type(:redis, self)

        include MonitorMixin

        attr_reader :service_name, :config, :logger

        Subscription = Struct.new(
          :channel,
          :mode,
          :block,
          keyword_init: true
        )

        def initialize(service_name:, service_id:, logger:, config: nil)
          super

          @config = validate_config(config)
          @stopped = true

          @subscriptions = []

          @simple_subscriber = Subscriber::Simple.new(
            redis: method(:with_redis),
            manager: self,
            service_name:,
            service_id:,
            prefix: "VERSE:RESOURCE:",
            &method(:dispatch_message)
          )

          @stream_subscriber = Subscriber::Stream.new(
            @config.stream,
            manager: self,
            consumer_name: service_name,
            consumer_id: service_id,
            shards: @config.partitions,
            prefix: "VERSE:STREAM:RESOURCE:",
            redis: method(:with_redis),
            &method(:dispatch_message)
          )
        end

        def start
          @stopped = false
          prepare_subscriptions

          @simple_subscriber.start
          @stream_subscriber.start
        end

        def stop
          @simple_subscriber.stop
          @stream_subscriber.stop
          @stopped = true
        end

        def restart
          stop
          start
        end

        def with_redis(&block)
          Verse::Plugin[@config.plugin_name].with_client(&block)
        end

        # Publish an event which happened to a specific resource.
        # This is useful to ensure ordering of events.
        # @param resource_type [String] The resource type/class
        # @param resource_id [String] The resource id
        # @param event [String] The event type
        # @param payload [Hash] The payload content of the event
        # @param headers [Hash] The headers of the message (if any)
        # @param reply_to [String] The channel to send the response to if any
        def publish_resource_event(resource_type:, resource_id:, event:, payload:, headers: {})
          shard = find_partition(resource_id)

          stream = ["VERSE:STREAM:RESOURCE", [resource_type, shard].join("$")].join(":")
          simple_channel = ["VERSE:RESOURCE", resource_type, event].join(":")

          headers = { event: }.merge(headers)

          message = Message.new(
            payload,
            manager: self,
            headers:
          )

          content = message.pack

          with_redis do |redis|
            # Add to the stream if any stream exists.

            redis.xadd(
              stream,
              { msg: content },
              nomkstream: true,
              approximate: true,
              maxlen: config.maxlen
            )

            # add to the fire and forget event stream
            redis.publish(simple_channel, content)
          end
        end

        # Publish an event to a specific channel.
        def publish(channel, content, headers: {}, key: nil, reply_to: nil)
          message = Message.new(content, manager: self, headers:, reply_to:)

          packed_message = message.pack

          with_redis do |rd|
            # Note for later: to improve bandwidth usage, we should use a LUA script
            # to prevent to pass the message twice.

            # Publish on non-persistent channel
            logger.debug{ "Publishing on channel #{channel} (#{packed_message.size} bytes)" }
            rd.publish(channel, packed_message)

            # Publish on persistent channel
            partition = key && find_partition(key)

            stream_config = @config.streams[channel.to_sym]
            max_len = stream_config&.maxlen || @config.maxlen

            channel = [channel, partition].compact.join("$")

            rd.xadd(
              channel,
              { msg: packed_message },
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
        def request(channel, content = {}, headers: {}, reply_to: nil, timeout: 0.5)
          reply_to ||= "REPLY_TO:#{SecureRandom.hex}"

          cond = new_cond
          q = Queue.new

          thread = nil

          msgpacked =
            Message.new(content, manager: self, headers:, reply_to:).pack

          synchronize do
            thread = Thread.new do
              synchronize{} # Stop the thread until the wait block is called below.
              with_redis do |rd|
                rd.subscribe_with_timeout(timeout, reply_to) do |on|
                  on.subscribe do
                    synchronize{ cond.signal }
                  end

                  on.message do |chan, message|
                    logger.debug { "Received reply message on #{chan}: #{message.size}" }
                    q.push(Message.unpack(self, message))
                  end
                end
              end
            end

            cond.wait # ensure that the subscription process done before sending the message
            with_redis{ |rd|
              logger.debug { "Publishing on `#{channel}` (#{msgpacked.size} bytes)" }
              rd.publish(channel, msgpacked)
            }
          end

          Timeout.timeout(timeout) do
            out = q.pop
            thread.kill
            out
          end
        end

        # Send request to multiple subscribers. Wait until timeout and
        # return an array of responses.
        # @param channel [String] The channel to send the request to
        # @param payload [Hash] The payload of the request
        # @param headers [Hash] The headers of the message (if any)
        # @param timeout [Float] The timeout of the request
        # @return Promise<[Array<Message>]> The responses of the request
        def request_all(channel, content = {}, headers: {}, reply_to: nil, timeout: 0.5)
          reply_to ||= "REPLY_TO:#{SecureRandom.hex}"

          output = []
          cond = new_cond
          thread = nil

          msgpacked =
            Message.new(content, manager: self, headers:, reply_to:).pack

          synchronize do
            thread = Thread.new do
              synchronize{} # Stop the thread until the wait block is called below.
              with_redis do |rd|
                rd.subscribe_with_timeout(timeout, reply_to) do |on|
                  on.subscribe do
                    synchronize{ cond.signal }
                  end

                  on.message do |chan, message|
                    logger.debug { "Received reply message on #{chan}: #{message.size}" }
                    synchronize{ output << Message.unpack(self, message) }
                  end
                end
              end
            end

            cond.wait # ensure that the subscription process done before sending the message

            with_redis{ |rd|
              logger.debug { "Publishing on `#{channel}` (#{msgpacked.size} bytes)" }
              rd.publish(channel, msgpacked)
            }
          end

          sleep(timeout)
          thread.kill
          output
        end

        # Subscribe to a specific channel in a specific mode
        # @param channel [String] The channel to subscribe to
        # @param mode [Symbol] The mode of the subscription
        # @param block [Proc] The block to execute when a message is received
        def subscribe(topic:, mode: Verse::Event::Manager::MODE_CONSUMER, &block)
          raise "cannot subscribe when started" unless @stopped

          unless Event::Manager::ALL_MODES.include?(mode)
            raise ArgumentError,
                  "mode must be #{Event::Manager::ALL_MODES.map(&:inspect).join(", ")}, but `#{mode}` given"
          end

          @subscriptions << Subscription.new(
            channel: topic,
            mode:,
            block:
          )
        end

        def subscribe_resource_event(resource_type:, event:, mode: Verse::Event::Manager::MODE_CONSUMER, &block)
          logger.debug { "Subscribe resource event #{resource_type}##{event} in mode #{mode}" }

          stream_id = \
            case mode
            when Event::Manager::MODE_CONSUMER
              ["VERSE:STREAM:RESOURCE", resource_type].join(":")
            else
              ["VERSE:RESOURCE", resource_type, event].join(":")
            end

          logger.debug { "Subscribe on #{stream_id}" }

          callback = lambda do |message, channel|
            next if message.headers[:event] != event

            block.call(message, channel)
          end

          @subscriptions << Subscription.new(
            channel: stream_id,
            mode:,
            block: callback
          )
        end

        def dispatch_message(channel, message)
          logger.debug { "Dispatch message #{channel} #{message}" }

          filter = lambda { |sub|
            sub.mode == Event::Manager::MODE_CONSUMER &&
              channel.start_with?(sub.channel) ||
              sub.channel == channel
          }

          @subscriptions.lazy.select(&filter).each do |sub|
            sub.block.call(message, channel)
          rescue StandardError => e
            logger.error { "Error while processing message on channel #{channel}: #{e.message}" }
            logger.error { e.backtrace.join("\n") }
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
          end % @config.partitions
        end

        def validate_config(config)
          result = Config::Schema.validate(config)

          return result.value if result.success?

          raise "Invalid config for redis plugin: #{result.errors}"
        end

        def prepare_subscriptions
          @subscriptions.each do |sub|
            case sub.mode
            when Event::Manager::MODE_CONSUMER
              @stream_subscriber.subscribe(sub.channel)
            when Event::Manager::MODE_COMMAND
              @simple_subscriber.subscribe(sub.channel, lock: true)
            when Event::Manager::MODE_BROADCAST
              @simple_subscriber.subscribe(sub.channel, lock: false)
            end
          end
        end
      end
    end
  end
end
