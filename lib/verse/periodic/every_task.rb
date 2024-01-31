# frozen_string_literal: true

module Verse
  module Periodic
    # A task that runs every period seconds.
    # The task is scheduled to run at the next period boundary.
    # Periods are aligned to the start of the epoch, meaning that
    # if you create a task with a period of 60 seconds, it will run
    # at the start of every minute (not in a minute from now).
    class EveryTask < Task
      def planned_time(now, period)
        (now - (now % period)) + period
      end

      def initialize(name, manager, period, per_service: true, &block)
        @per_service = per_service

        after do
          @at += period
          manager.add_task(self)
        end

        super(
          name,
          manager,
          planned_time(Time.now.to_f, period),
          per_service: per_service,
          &block
        )
      end
    end
  end
end
