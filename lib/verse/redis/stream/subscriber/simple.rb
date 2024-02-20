require_relative "./base"

module Verse
  module Redis
    module Stream
      module Subscriber
        class Simple < Base

          attr_reader :service_name, :service_id

          def initialize(redis:, service_name:, service_id:, &block)
            super(redis:, &block)
            @service_name = service_name
            @service_id = service_id
          end

          def start
            super
            @thread = Thread.new{ listen }
            @thread.name = "Verse Redis EM - Basic Subscriber"
          end

          def stop
            super
            @thread&.kill
          end

          def lock(redis, channel, &block)
            lock_key = "VERSE:STREAM:SIMPLE:LOCK:#{channel}:#{service_name}"

            redis.set(lock_key, service_id, nx: true, ex: 600, &block)

            yield if redis.get(lock_key) == service_id
          end

          def listen
            redis do |r|
              lock_set = channels.to_h
              r.subscribe(*lock_set.keys) do |on|
                on.subscribe do |channel, _|
                  Verse.logger.debug{ "Subscribed to `#{channel}`" }
                end

                on.message do |channel, message|
                  has_lock = lock_set[channel]

                  if has_lock
                    lock(r, channel) do
                      # per service message
                      Verse.logger.debug { "Message on `#{channel}` (#{message.size} bytes)" }
                      process_message(channel, message)
                    end
                  else
                    # broadcasted message
                    Verse.logger.debug { "Message on `#{channel}` (#{message.size} bytes)" }
                    process_message(channel, message)
                  end
                end
              end
            end
          rescue ::Redis::BaseConnectionError => error
            Verse.warn{ "#{error}, retrying in 0.5s" }
            sleep 0.5
            retry
          end

        end
      end
    end
  end
end