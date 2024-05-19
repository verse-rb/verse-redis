# frozen_string_literal: true

module Verse
  module Periodic
    module Locker
      # RedisLocker is a Verse::Periodic::Locker implementation that uses Redis
      # to coordinate task execution.
      class Base

        attr_reader :service_name, :service_id

        # Initialize the locker
        # @param service_name [String] the name of the micro-service (Verse.service_name)
        # @param service_id [String] the id of the micro-service (Verse.service_id)
        def initialize(service_name:, service_id:)
          @service_name = service_name
          @service_id = service_id
        end

        # Lock the task and run the block if the lock is acquired.
        def lock(name, at, &block)
          raise NotImplementedError
        end
      end
    end
  end
end

require_relative "./redis"
require_relative "./none"