# frozen_string_literal: true

require_relative "./periodic_hook"
require_relative "./schedule_hook"

module Verse
  module Periodic
    module Exposition
      ##
      # The extension module for Verse::Exposition::Base
      # @see Verse::Exposition::Base
      module Extension
        class << self
          attr_accessor :periodic_manager
        end

        UNITS = {
          second: 1,
          minute: 60,
          hour: 3600,
          day: 86_400,
          week: 604_800,
          month: 2_592_000,
        }.freeze

        def on_schedule(cron, per_service: true)
          ScheduleHook.new(
            self,
            Extension.periodic_manager,
            cron,
            per_service:
          )
        end

        # Define a hook on periodic event
        # @param interval [Integer] the interval between each execution
        # @param unit [Symbol] the unit of the interval. Default is :minute
        # @param per_service [Boolean] whether the hook should be executed once
        #        per service (true) or per service instance (false)
        def on_every(interval, unit = :minute, per_service: true)
          total_time = interval * UNITS.fetch(unit) do
            raise ArgumentError, "Unknown unit: #{unit}"
          end

          PeriodicHook.new(
            self,
            Extension.periodic_manager,
            total_time,
            per_service:
          )
        end

        def one_time(delay: 0.0)
          OneTimeHook.new(
            self,
            Extension.periodic_manager,
            delay
          )
        end
      end
    end
  end
end
