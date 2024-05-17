# frozen_string_literal: true

require_relative "./redis_locker"

module Verse
  module Periodic
    # The Manager is responsible for scheduling tasks and executing them
    # at the appropriate time.
    # It is implemented as a thread that sleeps until the next task is due.
    # It is also responsible for locking tasks that are marked as per-service,
    # using the underlying locker.
    class Manager < Thread
      attr_reader :locker

      include MonitorMixin

      def initialize(locker = nil)
        self.name = "Verse::Periodic - Scheduler"

        @locker = locker
        @locker ||= RedisLocker.new(
          service_name: Verse.service_name,
          service_id: Verse.service_id,
          redis: proc do |&block|
            Verse::Plugin[:redis].with_client(&block)
          end
        )
        @task_list = []
        @cond = new_cond
        @executioner = Executioner.new

        super(&method(:run))
      end

      def run
        @stopped = false

        until @stopped
          synchronize do
            now = Time.now.to_f

            next_task = @task_list.first

            if next_task.nil?
              @cond.wait
            elsif next_task.at < now
              @task_list.shift
              @executioner.call(next_task)
            else
              @cond.wait(next_task.at - now)
            end
          end
        end
      end

      def lock(name, at, &block)
        @locker.lock(name, at, &block)
      end

      def add_task(task)
        synchronize do
          insert_index = @task_list.bsearch_index do |other_task|
            other_task.at >= task.at
          end

          if insert_index.nil?
            @task_list << task
          else
            @task_list.insert(insert_index, task)
          end

          @cond.signal
        end
      end

      def empty?
        synchronize { @task_list.empty? }
      end

      def stop
        synchronize do
          return if @stopped

          @executioner.stop
          @stopped = true
          @cond.signal
        end
      end
    end
  end
end
