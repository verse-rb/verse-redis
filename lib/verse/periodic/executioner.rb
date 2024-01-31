# frozen_string_literal: true

module Verse
  module Periodic
    # Executioner is a thread that executes periodic tasks.
    # It is used by the manager.
    class Executioner < Thread
      def initialize
        self.name = "Verse::Periodic - Executioner"
        @queue = SizedQueue.new(10)
        super(&method(:run))
      end

      def call(block)
        @queue << block
      end

      def stop
        @queue.close
      end

      def empty?
        @queue.empty?
      end

      def run
        # rubocop:disable Lint/RescueException
        while (block = @queue.pop)
          begin
            block.call
          rescue Exception => e
            Verse.logger.warn "Error in periodic task:"
            Verse.logger.warn e
          end
        end
        # rubocop:enable Lint/RescueException
      end
    end
  end
end
