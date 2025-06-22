# frozen_string_literal: true

require "securerandom"
require "verse/distributed/lock"
require_relative "./lock_config"

module Verse
  module Redis
    class Lock
      include Verse::Distributed::Lock

      attr_reader :config

      # Lua script for safely releasing a lock.
      # It checks if the key exists and its value matches the token before deleting it.
      RELEASE_SCRIPT = <<~LUA
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("del", KEYS[1])
        else
          return 0
        end
      LUA

      def initialize(config = {})
        super
        @config = Verse::Redis::LockConfig.new(config)
        @plugin = Verse::Plugin[@config.plugin]
      end

      def acquire(lock_key, requested_ttl_ms, acquire_timeout_ms)
        token = SecureRandom.hex(32)
        deadline = Time.now + (acquire_timeout_ms / 1000.0)
        lock_key = prefixed_key(lock_key)

        loop do
          return token if try_acquire(lock_key, token, requested_ttl_ms)

          return nil if Time.now >= deadline
          sleep(0.01)
        end
      end

      def release(lock_key, lock_token)
        @plugin.with_client do |redis|
          redis.eval(RELEASE_SCRIPT, keys: [prefixed_key(lock_key)], argv: [lock_token]) > 0
        end
      end

      def renew(lock_key, lock_token, new_ttl_ms)
        # This is a simple implementation. A more robust one would use a Lua script
        # to check the token and set the new TTL atomically.
        @plugin.with_client do |redis|
          lock_key = prefixed_key(lock_key)
          if redis.get(lock_key) == lock_token
            redis.pexpire(lock_key, new_ttl_ms)
            true
          else
            false
          end
        end
      end

      private

      def try_acquire(lock_key, token, ttl_ms)
        @plugin.with_client do |redis|
          redis.set(lock_key, token, nx: true, px: ttl_ms)
        end
      end

      def prefixed_key(key)
        [@config.key_prefix, key].compact.reject(&:empty?).join(":")
      end
    end
  end
end
