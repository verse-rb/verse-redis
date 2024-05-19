# frozen_string_literal: true

module Verse
  module Periodic
    module Locker
      # RedisLocker is a Verse::Periodic::Locker implementation that uses Redis
      # to coordinate task execution.
      class None < Base
        # Lock the task and run the block if the lock is acquired.
        def lock(name, at, &block)
          yield
        end
      end
    end
  end
end