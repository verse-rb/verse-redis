# frozen_string_literal: true

module Verse
  module Periodic
    # RedisLocker is a Verse::Periodic::Locker implementation that uses Redis
    # to coordinate task execution.
    class RedisLocker
      DEFAULT_KEY = "VERSE:PERIODIC:LOCK:%<service_name>s:%<key>s:%<at>s"

      attr_reader :service_name, :service_id

      def initialize(service_name:, service_id:, redis:, key: DEFAULT_KEY, expire: 86_400)
        @key = key
        @expire = expire
        @redis_block = redis.is_a?(Method) || redis.is_a?(Proc) ? redis : ->(&block) { block.call(redis) }

        @service_name = service_name
        @service_id = service_id
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

        # Yield outside of with_client block to release redis connection.
        block.call if lock_acquired
      end
    end
  end
end
