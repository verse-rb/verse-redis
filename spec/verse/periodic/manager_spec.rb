# frozen_string_literal: true

RSpec.describe Verse::Periodic::Manager do
  before do
    @manager = Verse::Periodic::Manager.new
  end

  after do
    @manager.stop
  end

  it "run the tasks in the correct order" do
    the_string = "Verse Periodic is Awesome. We can do anything with it."

    output = String.new

    tasks = the_string.chars.map.with_index do |char, index|
      Verse::Periodic::Task.new("test", nil, index) do
        output << char
      end
    end

    tasks.shuffle! # Disorder on purpose

    tasks.each do |task|
      @manager.add_task(task)
    end

    sleep 0.01 until @manager.empty?

    expect(output).to eq(the_string)
  end

  it "wait before running a task" do
    now = Time.now.to_f
    output = nil
    task = Verse::Periodic::Task.new("test", @manager, now + 0.05) do
      output = "Hello"
    end

    @manager.add_task(task)
    sleep 0.01
    expect(output).to be_nil
    sleep 0.041
    expect(output).to eq("Hello")
  end

  it "should call the lock per service" do
    now = Time.now.to_f
    expected_date = now + 0.05

    lock_called = false

    stub_locker = double("locker")
    allow(stub_locker).to receive(:lock) do |name, at, &block|
      lock_called = true
      expect(name).to eq("test")
      expect(at).to be_within(0.001).of(expected_date)
      block.call
    end

    @manager.stop
    @manager = Verse::Periodic::Manager.new(stub_locker)

    output = nil
    task = Verse::Periodic::Task.new("test", @manager, expected_date, per_service: true) do
      output = "Hello"
    end

    @manager.add_task(task)

    sleep 0.01
    expect(output).to be_nil
    sleep 0.041
    expect(output).to eq("Hello")

    expect(lock_called).to be(true)

    @manager.stop
  end


end
