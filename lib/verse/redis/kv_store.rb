# frozen_string_literal: true

require "verse/distributed/kv_store"
require_relative "./kv_config"

module Verse
  module Redis
    # Redis implementation of Verse::Distributed::KVStore
    class KVStore
      include Verse::Distributed::KVStore

      attr_reader :config

      def initialize(config = {})
        super
        @config = Verse::Redis::KVConfig.new(config)
        @plugin = Verse::Plugin[@config.plugin]
      end

      def get(key)
        @plugin.with_client do |redis|
          redis.get(prefixed_key(key))
        end
      end

      def set(key, value, ttl: nil)
        @plugin.with_client do |redis|
          if ttl
            redis.setex(prefixed_key(key), ttl, value)
          else
            redis.set(prefixed_key(key), value)
          end
        end
      end

      def delete(key)
        @plugin.with_client do |redis|
          redis.del(prefixed_key(key)) > 0
        end
      end

      def clear_all
        @plugin.with_client do |redis|
          redis.scan_each(match: "#{prefixed_key("*")}") do |key|
            redis.del(key)
          end
        end
      end

      private

      def prefixed_key(key)
        [@config.key_prefix, key].compact.reject(&:empty?).join(":")
      end
    end
  end
end
