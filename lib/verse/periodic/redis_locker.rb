# frozen_string_literal: true

module Verse
  module Periodic
    # RedisLocker is a Verse::Periodic::Locker implementation that uses Redis
    # to coordinate task execution.
    class RedisLocker
      DEFAULT_KEY = "verse:periodic:tasks:%<service_name>s:%<key>s:%<at>s"

      def initialize(key: DEFAULT_KEY, expire: 86_400, plugin_name: :redis)
        @key = key
        @expire = expire
        @plugin_name = plugin_name
      end

      def lock(name, at, &block)
        raise ArgumentError, "block is required" unless block_given?

        lock_acquired = false
        Verse.plugins[@plugin_name].with_client do |redis|
          exp = Time.now + @expire # keep the key 24 hours
          key = format(@key, service_name: Verse.service_name, key: name, at: at)
          lock_acquired = redis.set(key, "1", nx: true, ex: exp)
        end

        # Yield outside of with_client block to release redis connection.
        block.call if lock_acquired
      end
    end
  end
end
