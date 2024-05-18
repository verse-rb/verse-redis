# frozen_string_literal: true

require_relative "../run_once_task"

module Verse
  module Periodic
    module Exposition
      class OneTimeHook < Verse::Exposition::Hook::Base
        attr_reader :delay, :manager

        # Create a new hook
        # Used internally by the `one_time` method.
        # @see Verse::Periodic::Exposition::Extension#one_time
        def initialize(exposition, manager, delay)
          super(exposition)
          @delay = delay
          @manager = manager
        end

        # :nodoc:
        def register_impl
          hook = self

          task_name = [
            Verse.service_name,
            exposition_class.name,
            method.original_name
          ].join(":")

          task = RunOnceTask.new(
            task_name, @manager, @delay
          ) do
            context = Verse::Auth::Context[:system]
            context.mark_as_checked!

            exposition = hook.create_exposition(
              context, schedule: task
            )

            exposition.run do
              hook.method.bind(exposition).call
            end
          end

          Verse.on_boot { manager.add_task(task) }
        end
      end
    end
  end
end
