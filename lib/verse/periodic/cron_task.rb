# frozen_string_literal: true

require "fugit"

module Verse
  module Periodic
    # CronTask is a task that runs at a specific time, specified by a cron
    # expression.
    class CronTask < Task
      def per_service?
        @per_service
      end

      attr_reader :cron_rule, :at, :per_service

      def initialize(name, manager, cron, per_service: true, &block)
        @per_service = per_service

        @cron_rule = Fugit.do_parse_cronish(cron)
        now = Time.now

        next_trigger = @cron_rule.next(now).first
        @at = next_trigger.to_f

        after do
          @at = @cron_rule.next(@at + 0.01).first.to_f
          manager.add_task(self)
        end

        super(
          name,
          manager,
          @at,
          per_service:,
          &block
        )
      end
    end
  end
end
