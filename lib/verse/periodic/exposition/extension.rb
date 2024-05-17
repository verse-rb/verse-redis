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

        def on_schedule(cron, per_service: true)
          ScheduleHook.new(
            self,
            Extension.periodic_manager,
            cron,
            per_service:
          )
        end
      end
    end
  end
end
