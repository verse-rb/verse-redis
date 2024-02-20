require_relative "./base"

module Verse
  module Redis
    module Stream
      module Subscriber
        class Basic < Base

          def start
            super
            @thread = Thread.new{ listen }
            @thread.name = "Verse Redis EM - Basic Subscriber"
          end

          def stop
            super
            @thread&.kill
          end

          def lock(redis, &block)
            lock_key = "VERSE:STREAM:BASICLOCK:#{Verse.service_name}"

            redis.set(lock_key, Verse.service_id, nx: true, ex: 600, &block)

            yield if redis.get(lock_key) == Verse.service_id
          end

          def listen
            redis do |r|
              lock_set = channels.to_h
              r.subscribe(*lock_set.keys) do |on|
                on.message do |channel, message|
                  puts "##{channel}: #{message}"

                  has_lock = lock_set[channel]

                  if has_lock
                    lock(r, channel) do
                      # per service message
                      process_message(channel, message)
                    end
                  else
                    # broadcasted message
                    process_message(channel, message)
                  end
                end
              end
            end
          rescue Redis::BaseConnectionError => error
            Verse.warn{ "#{error}, retrying in 0.5s" }
            sleep 0.5
            retry
          end

        end
      end
    end
  end
end