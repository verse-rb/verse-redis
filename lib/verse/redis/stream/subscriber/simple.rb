require_relative "./base"
require "monitor"

module Verse
  module Redis
    module Stream
      module Subscriber
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

            synchronize do
              @thread = Thread.new{ listen }
              @thread.name = "Verse Redis EM - Basic Subscriber"

              @cond.wait
            end
          end

          def stop
            super
            @thread&.kill
          end

          def lock(redis, channel, msgid, &block)
            lock_key = "VERSE:STREAM:SIMPLE:LOCK:#{channel}:#{service_name}:#{msgid}"

            redis.set(lock_key, service_id, nx: true, ex: 600, &block)

            yield if redis.get(lock_key) == service_id
          end

          def listen
            redis do |r|
              lock_set = channels.to_h
              r.subscribe(*lock_set.keys) do |on|
                on.subscribe do |channel, _|
                  synchronize do
                    Verse.logger.debug{ "Subscribed to `#{channel}`" }
                    @cond.signal # Signal that subscription is ready. #start method will stop waiting
                  end
                end

                on.message do |channel, message|
                  unpacked_message = Message.unpack(@manager, message, channel: channel)

                  has_lock = lock_set[channel]

                  if has_lock
                    lock(r, channel, unpacked_message.id) do
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
              end
            end
          rescue ::Redis::BaseConnectionError => error
            Verse.warn{ "#{error}, retrying in 500ms" }
            sleep 0.5
            retry
          end

        end
      end
    end
  end
end