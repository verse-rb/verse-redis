# frozen_string_literal: true

require "monitor"
require "verse/core"

require_relative "./config"

module Verse
  module Redis
    # Redis plugin for Verse
    # Have a method called `with_redis` that will yield a redis connection
    # from a pool of connections of dimension defined by the configuration.
    class Plugin < Verse::Plugin::Base
      attr_reader :config, :connection_list

      include MonitorMixin

      def description
        "Redis integration to Verse"
      end

      def on_init
        begin
          require "redis"
        rescue LoadError
          Verse.logger.error "Please add `redis` to your Gemfile!"
          raise
        end

        @thread_local_key = :"__redis_plugin_#{object_id}"
        @connection_pool = Queue.new
        @connection_list = []
        @connection_count = 0
        @config = validate_config
      end

      def with_client(&block)
        cnx = Thread.current[@thread_local_key]

        if cnx
          # Prevent reallocating a connection in the same thread
          # if two with_redis block are nested
          block.call(cnx)
        else
          begin
            cnx = request_connection
            Thread.current[@thread_local_key] = cnx
            block.call(cnx)
          ensure
            Thread.current[@thread_local_key] = nil
            release_connection(cnx) if cnx
          end
        end
      end

      def on_stop
        @connection_pool.close
        @connection_list.each(&:close)
      end

      protected

      def request_connection
        synchronize do
          if @connection_count < @config.max_connections
            @connection_count += 1
            cnx = create_connection
            @connection_list << cnx
            cnx
          else
            @connection_pool.pop
          end
        end
      end

      def release_connection(cnx)
        @connection_pool << cnx
      end

      def validate_config
        result = Verse::Redis::Config::Schema.validate(config)

        return result.value if result.success?

        raise "Invalid config for redis plugin: #{result.errors}"
      end
    end
  end
end
