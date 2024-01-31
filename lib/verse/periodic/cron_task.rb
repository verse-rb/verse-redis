# frozen_string_literal: true

module Verse
  module Periodic
    # CronTask is a task that runs at a specific time, specified by a cron
    # expression.
    class CronTask < Task
      def initialize(name, manager, cron, per_service: true, &block)
        @work = work
        @per_service = per_service
        @cron_rule = CronParser.new(cron)
        @cron_rule.next(Time.now)

        after do
          @at = @cron_rule.next(@at + 1)
          manager.add_task(self)
        end

        super(
          name,
          manager,
          @at,
          per_service: per_service,
          &block
        )
      end
    end
  end
end
