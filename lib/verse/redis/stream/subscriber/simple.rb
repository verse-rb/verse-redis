# frozen_string_literal: true

require_relative "./base"
require "monitor"

module Verse
  module Redis
    module Stream
      module Subscriber
        # A simple subscriber using Redis Pub/Sub feature
        class Simple < Base
          include MonitorMixin

          attr_reader :service_name, :service_id

          def initialize(redis:, manager:, service_name:, service_id:, &block)
            super(redis:, manager:, &block)

            @service_name = service_name
            @service_id = service_id

            @cond = new_cond
          end

          def start
            super

            return if channels.empty?

            @lock_set = channels.to_h

            synchronize do
              @thread = Thread.new { listen }
              @thread.name = "Verse Redis EM - Simple Subscriber"

              @cond.wait
            end
          end

          def stop
            super
            @thread&.kill
          end

          def lock(channel, msgid)
            redis do |r|
              lock_key = "VERSE:STREAM:SIMPLE:LOCK:#{channel}:#{service_name}:#{msgid}"

              r.set(lock_key, service_id, nx: true, ex: 600)
              yield if r.get(lock_key) == service_id.to_s
            end
          end

          def on_subscribe(channel, _)
            synchronize do
              Verse.logger.debug { "Subscribed to `#{channel}`" }
              @cond.signal # Signal that subscription is ready. #start method will stop waiting
            end
          end

          def on_message(channel, message)
            unpacked_message = Message.unpack(@manager, message, channel:)

            has_lock = @lock_set[channel]

            if has_lock
              Verse.logger.debug { "Try lock on `#{channel}` (#{unpacked_message.id})" }
              lock(channel, unpacked_message.id) do
                # per service message
                Verse.logger.debug { "Message on `#{channel}` (#{message.size} bytes)" }
                process_message(channel, unpacked_message)
              end
            else
              # broadcasted message
              Verse.logger.debug { "Message on `#{channel}` (#{message.size} bytes)" }
              process_message(channel, unpacked_message)
            end
          end

          def listen
            # ensure that the thread doesn't start before condition#wait is called.
            # This case is happening randomly and very rarely, as we don't have
            # control over the thread scheduling.
            # That's could potentially ruin everything and is so hard to debug,
            # leading to the process hanging forever :(
            synchronize{}

            redis do |r|
              r.subscribe(*@lock_set.keys) do |on|
                on.subscribe(&method(:on_subscribe))
                on.message(&method(:on_message))
              end
            end
          rescue ::Redis::BaseConnectionError => e
            Verse.warn { "#{e}, retrying in 500ms" }
            sleep 0.5
            retry
          end
        end
      end
    end
  end
end
