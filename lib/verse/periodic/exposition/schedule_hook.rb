# frozen_string_literal: true

require "csv"

require_relative "../cron_task"

module Verse
  module Periodic
    module Exposition
      # Define a hook on cron schedule event
      # @see Verse::Periodic::Exposition::Extension#on_schedule
      # @see Verse::Exposition::Base#expose
      class ScheduleHook < Verse::Exposition::Hook::Base
        attr_reader :cron, :manager

        def per_service?
          @per_service
        end

        # Create a new hook
        # Used internally by the `on_schedule` method.
        # @see Verse::Periodic::Exposition::Extension#on_schedule
        def initialize(exposition, manager, cron, per_service:)
          super(exposition)
          @cron = cron
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

          CronTask.new(
            task_name, @manager, @cron, per_service: @per_service
          ) do
            binding.pry
            hook.call
          end
        end
      end
    end
  end
end
