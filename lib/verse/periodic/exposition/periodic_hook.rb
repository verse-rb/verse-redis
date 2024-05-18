# frozen_string_literal: true

require_relative "../every_task"

module Verse
  module Http
    module Exposition
      # Define a hook on cron schedule event
      # @see Verse::Periodic::Exposition::Extension#on_every
      # @see Verse::Exposition::Base#expose
      class PeriodicHook < Verse::Exposition::Hook::Base
        attr_reader :interval, :manager

        def per_service?
          @per_service
        end

        # Create a new hook
        # Used internally by the `on_every` method.
        # @see Verse::Periodic::Exposition::Extension#on_every
        def initialize(exposition, manager, interval, per_service:)
          super(exposition)
          @interval = interval
          @manager = manager
          @per_service = per_service
        end

        # :nodoc:
        def register_impl
          hook = self

          task_name = [
            Verse.service_name,
            exposition_class.name,
            method.original_name
          ].join(":")


          EveryTask.new(
            task_name, @manager, @cron, per_service: @per_service
          ) do
            hook.call
          end
        end
      end
    end
  end
end
