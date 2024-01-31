# frozen_string_literal: true

module Verse
  module Periodic
    # Task which is ran once and then removed from the manager.
    # Task running once are always "run once" on each instance; they
    # can't use the per_service option.
    class RunOnceTask < Task
      def initialize(name, manager, delay = 0, &block)
        super(
          name,
          manager,
          Time.now.to_f + delay,
          per_service: false,
          &block
        )
      end
    end
  end
end
