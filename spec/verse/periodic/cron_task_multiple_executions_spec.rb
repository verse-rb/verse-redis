# frozen_string_literal: true

RSpec.describe Verse::Periodic::CronTask do
  context "multiple executions" do
    it "should execute the task multiple times according to the cron schedule" do
      locker = double("locker")
      allow(locker).to receive(:lock) do |_name, _at, &block|
        block.call
      end

      manager = Verse::Periodic::Manager.new(locker)

      execution_count = 0
      execution_times = []

      task = Verse::Periodic::CronTask.new(
        "test_multiple_executions",
        manager,
        "* * * * * *", # Every second
        per_service: false # Avoid locking complexity for this test
      ) do
        execution_count += 1
        execution_times << Time.now.to_f
      end

      # Add the task to the manager
      manager.add_task(task)

      # We'll wait up to 3.01 seconds to see at least 3 executions
      wait_until = Time.now.to_f + 3.01

      expect(execution_count).to eq(0)

      # Wait and check periodically
      while Time.now.to_f < wait_until && execution_count < 3
        sleep 0.1
      end

      manager.stop

      expect(execution_count).to be >= 3

      time_diffs = execution_times.each_cons(2).map { |a, b| b - a }
      time_diffs.each do |diff|
        expect(diff).to be_within(0.5).of(1.0)
      end
    end
  end
end
