# frozen_string_literal: true

require_relative "../every_task"

module Verse
  module Http
    module Exposition
      class OneTimeHook < Verse::Exposition::Hook::Base
        attr_reader :delay, :manager

        def per_service?
          @per_service
        end

        # Create a new hook
        # Used internally by the `one_time` method.
        # @see Verse::Periodic::Exposition::Extension#one_time
        def initialize(exposition, manager, delay, per_service:)
          super(exposition)
          @delay = delay
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

          Verse.on_boot {
            manager.add_task(
              OneTimeTask.new(
                task_name, @manager, @delay, per_service: @per_service
              ){ hook.call }
            )
          }
          end
        end
      end
    end
  end