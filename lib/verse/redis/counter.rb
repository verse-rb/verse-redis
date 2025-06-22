# frozen_string_literal: true

require "verse/distributed/counter"
require_relative "./counter_config"

module Verse
  module Redis
    class Counter
      include Verse::Distributed::Counter

      attr_reader :config

      def initialize(config = {})
        super
        @config = Verse::Redis::CounterConfig.new(config)
        @plugin = Verse::Plugin[@config.plugin]
      end

      def increment(counter_name, amount = 1, ttl: nil)
        @plugin.with_client do |redis|
          key = prefixed_key(counter_name)
          new_value = redis.incrby(key, amount)
          redis.expire(key, ttl) if ttl
          new_value
        end
      end

      def get(counter_name)
        @plugin.with_client do |redis|
          redis.get(prefixed_key(counter_name))&.to_i
        end
      end

      def set(counter_name, value, ttl: nil)
        @plugin.with_client do |redis|
          key = prefixed_key(counter_name)
          if ttl
            redis.setex(key, ttl, value)
          else
            redis.set(key, value)
          end
        end
      end

      def delete(counter_name)
        @plugin.with_client do |redis|
          redis.del(prefixed_key(counter_name)) > 0
        end
      end

      def exists?(counter_name)
        @plugin.with_client do |redis|
          redis.exists?(prefixed_key(counter_name))
        end
      end

      private

      def prefixed_key(key)
        [@config.key_prefix, key].compact.reject(&:empty?).join(":")
      end
    end
  end
end
