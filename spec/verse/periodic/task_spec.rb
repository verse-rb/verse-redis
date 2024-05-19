# frozen_string_literal: true

RSpec.describe Verse::Periodic::Task do
  let(:manager) do
    manager = double("manager")

    allow(manager).to receive(:lock) do |_name, _at, &block|
      @lock_called = true
      block.call
    end

    manager
  end

  context "attributes" do
    it "read attributes correctly" do
      task = Verse::Periodic::Task.new(
        "task_name",
        manager, # fake data
        1234,
        per_service: true,
      ) {}

      expect(task.name).to eq("task_name")
      expect(task.manager).to eq(manager)
      expect(task.at).to eq(1234)
      expect(task.per_service?).to eq(true)
    end
  end

  context "#call" do
    it "run block when call" do
      task = Verse::Periodic::Task.new(
        "task_name",
        manager,
        1234,
        per_service: true,
      ) { "block called" }

      expect(task.call).to eq("block called")
    end

    it "run after block when call" do
      output = nil
      task = Verse::Periodic::Task.new(
        "task_name",
        manager, # fake data
        1234,
        per_service: true,
      ) { "block called" }

      task.after { output = "after block called" }

      expect(task.call).to eq("block called")
      expect(output).to eq("after block called")
    end
  end
end
