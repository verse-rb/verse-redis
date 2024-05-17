RSpec.describe Verse::Periodic::CronTask do
  subject do
    Verse::Periodic::CronTask.new(
      "task_name",
      manager,
      "*/5 * * * *",
      per_service: true,
    ) { "block called" }
  end

  let(:manager) do
    manager = double("manager")

    expect(manager).to receive(:lock) do |name, at, &block|
      expect(at.to_i).to be >= Time.now.to_i
      expect(at.to_i % 300).to eq(0) # Every 5 minutes
      expect(name).to eq("task_name")

      @lock_called = true
      block.call
    end

    expect(manager).to receive(:add_task) do |task|
      expect(task).to eq(subject)
    end

    manager
  end

  context "#call" do
    it "run block when call" do
      expect(subject.call).to eq("block called")
      expect(@lock_called).to eq(true)
    end

  end
end