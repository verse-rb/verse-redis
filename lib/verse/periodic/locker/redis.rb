# frozen_string_literal: true

module Verse
  module Periodic
    module Locker
      # RedisLocker is a Verse::Periodic::Locker implementation that uses Redis
      # to coordinate task execution.
      class Redis < Base
        attr_reader :key, :expire

        def initialize(
          service_name:, service_id:,
          redis: :redis,
          key: "VERSE:PERIODIC:LOCK:%<service_name>s:%<key>s:%<at>s",
          expire: 86_400
        )
          @key = key
          @expire = expire

          case redis
          when Method, Proc
            @redis_block = redis
          when Symbol
            @redis_block = ->(&block) { Verse::Plugin[redis].with_client(&block) }
          when ::Redis
            @redis_block = ->(&block) { block.call(redis) }
          else
            raise ArgumentError, "redis must be a Method, Proc, or Symbol"
          end

          super(service_name:, service_id:)
        end

        def lock(name, at, &block)
          raise ArgumentError, "block is required" unless block_given?

          lock_acquired = false
          @redis_block.call do |redis|
            exp = Time.now + @expire # keep the key 24 hours
            key = format(@key, service_name:, key: name, at:)
            redis.set(key, service_id, nx: true, ex: exp)
            lock_acquired = (redis.get(key) == service_id)
          end

          # Yield outside of with_client block to release the redis connection
          # if the block takes time to complete...
          block.call if lock_acquired
        end
      end
    end
  end
end
