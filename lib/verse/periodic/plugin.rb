# frozen_string_literal: true

require "monitor"
require "verse/core"

require_relative "./executioner"
require_relative "./manager"
require_relative "./task"
require_relative "./config"

require_relative "./exposition/extension"

module Verse
  module Periodic
    # Add periodic hooks to the Verse expositions
    class Plugin < Verse::Plugin::Base
      include Verse::Util::Reflection

      attr_reader :config

      # :nocov:
      def description
        "Periodic hooks for Verse. Cron and interval based hooks."
      end
      # :nocov:

      def dependencies
        %i[redis]
      end

      def on_init
        @config = validate_config

        locker_class = constantize(config.locker_class)

        opts = {
          service_name: Verse.service_name,
          service_id: Verse.service_id
        }

        opts.merge!(config.locker_config)

        locker = locker_class.new(**opts)

        @manager = Manager.new(locker)

        Exposition::Extension.periodic_manager = @manager

        Verse::Exposition::ClassMethods.prepend(
          Exposition::Extension
        )
      end

      def validate_config
        result = Verse::Periodic::Config::Schema.validate(@config)

        return result.value if result.success?

        raise "Invalid config for redis plugin: #{result.errors}"
      end

      def on_stop
        @manager.stop
      end
    end
  end
end
