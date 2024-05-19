# frozen_string_literal: true

require "monitor"
require "verse/core"

require_relative "./executioner"
require_relative "./manager"
require_relative "./task"

require_relative "./exposition/extension"

module Verse
  module Periodic
    # Add periodic hooks to the Verse expositions
    class Plugin < Verse::Plugin::Base
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
        @manager = Manager.new(
          RedisLocker.new(
            service_name: Verse.service_name,
            service_id: Verse.service_id,
            redis: redis.method(:with_client)
          )
        )

        Exposition::Extension.periodic_manager = @manager

        Verse::Exposition::ClassMethods.prepend(
          Exposition::Extension
        )
      end

      def on_stop
        @manager.stop
      end
    end
  end
end
