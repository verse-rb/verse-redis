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
        "Redis Stream Event Manager"
      end

      def dependencies
        %i<redis>
      end

      def on_init
        @config = validate_config
        @event_manager = EventManager.new(@config)
      end

      protected

      def validate_config
        result = Verse::Redis::Stream::Config::Schema.call(@config)
        return result.value if result.success?
        raise "Invalid config for redis plugin: #{result.errors}"
      end

    end
  end
end
