# frozen_string_literal: true

require_relative "./cache_config"

module Verse
  module Redis
    class CacheAdapter
      attr_reader :config, :plugin

      def initialize(config = {})
        @config = CacheConfig.new(config)
        @plugin = Verse::Plugin[@config.plugin]
      end

      def fetch(key, selector)
        plugin.with_client do |redis|
          redis.get(build_key(key, selector))
        end
      end

      def cache(key, selector, data, ex: nil)
        plugin.with_client do |redis|
          if ex
            redis.setex(build_key(key, selector), ex, data)
          else
            redis.set(build_key(key, selector), data)
          end
        end
      end

      def remove(key, selector)
        plugin.with_client do |redis|
          redis.del(build_key(key, selector))
        end
      end

      def flush(key, selectors)
        plugin.with_client do |redis|
          selectors.each do |selector|
            if selector == "*"
              redis.scan_each(match: build_key(key, "*")) do |k|
                redis.del(k)
              end
            else
              remove(key, selector)
            end
          end
        end
      end

      private

      def build_key(key, selector)
        [config.key_prefix, key, selector].join(":")
      end
    end
  end
end
