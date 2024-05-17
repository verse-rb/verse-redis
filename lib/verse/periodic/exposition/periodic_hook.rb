# frozen_string_literal: true

require "csv"

module Verse
  module Http
    module Exposition
      # A hook is a single endpoint on the http server
      # @see Verse::Http::Exposition::Extension#on_http
      # @see Verse::Exposition::Base#expose
      class PeriodicHook < Verse::Exposition::Hook::Base
        attr_reader :type

        # Create a new hook
        # Used internally by the `on_schedule` method.
        # @see Verse::Http::Exposition::Extension#on_http
        def initialize(exposition, manager, cron, per_service:)
          super(exposition)
          @cron = cron
          @manager = manager
          @per_service = per_service
        end

        # :nodoc:
        def register_impl
          hook = self

          binding.pry

          CronTask.new(
            "todo", @manager, @cron, per_service: @per_service
          ) do
            hook.call
          end
        end
      end
    end
  end
end
