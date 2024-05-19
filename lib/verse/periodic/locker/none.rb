# frozen_string_literal: true

module Verse
  module Periodic
    module Locker
      # Locker::None is a Verse::Periodic::Locker::Base implementation
      # that do nothing. Useful for test environments.
      class None < Base
        # Lock the task and run the block if the lock is acquired.
        def lock(*)
          yield
        end
      end
    end
  end
end
