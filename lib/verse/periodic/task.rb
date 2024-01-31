# frozen_string_literal: true

module Verse
  module Periodic
    # Task is a task that runs at a specific time, based on the underlying
    # implementation it can be runned using Cron or Every period of time, or
    # run once on starting your Verse Application.
    class Task
      attr_reader :name, :manager, :at, :block

      def initialize(name, manager, at, per_service: false, &block)
        @name = name
        @manager = manager
        @at = at
        @block = block
        @per_service = per_service
      end

      def after(&block)
        @after = block
      end

      def call
        if @per_service
          manager.lock(@name, @at, &@block)
        else
          @block.call
        end
      ensure
        @after&.call
      end
    end
  end
end
