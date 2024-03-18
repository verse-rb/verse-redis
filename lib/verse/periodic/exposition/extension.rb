# frozen_string_literal: true

require_relative "./hook"

module Verse
  module Periodic
    module Exposition
      ##
      # The extension module for Verse::Http::Exposition::Base
      # @see Verse::Http::Exposition::Base
      module Extension

        class << self
          attr_accessor :periodic_manager
        end

        def on_schedule(cron)
          hook = Hook.new(self, :schedule, cron)
          hook.register
        end

      end
    end
  end
end
