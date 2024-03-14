require "verse/redis/stream/subscriber/simple"
require "redis"

RSpec.describe Verse::Redis::Stream::Subscriber::Simple do
  around(:each) do |example|
    protected_methods = described_class.protected_instance_methods
    private_methods = described_class.private_instance_methods

    begin
      described_class.send(:public, *protected_methods, *private_methods)
      example.run
    ensure
      described_class.send(:protected, *protected_methods)
      described_class.send(:private, *private_methods)
    end
  end

  before do
    redis.flushall
    @messages = {}
  end

  let(:redis) { Redis.new }

  let(:queue) { Queue.new }

  let(:message) { Verse::Redis::Stream::Message.new("This is a message")}

  subject(:subscriber_a) do
    described_class.new(
      redis: Redis.new,
      manager: nil,
      service_name: "test:service",
      service_id: "a",
    ) do |channel, message|
      (@messages[channel] ||= []) << message
      queue << message
    end
  end

  subject(:subscriber_b) do
    described_class.new(
      redis: Redis.new,
      manager: nil,
      service_name: "test:service",
      service_id: "b",
    ) do |channel, message|
      (@messages[channel] ||= []) << message
      queue << message
    end
  end

  context "publish/subscribe" do
    it "Receive broadcast messages" do
      subscriber_a.subscribe("test_channel", lock: false)
      subscriber_b.subscribe("test_channel", lock: false)

      subscriber_a.start
      subscriber_b.start

      sleep(0.1) # Wait for subscription to start

      redis.publish("test_channel", message.pack)

      # Received for each subscriber:
      queue.pop
      queue.pop

      expect(@messages["test_channel"].map(&:content)).to eq([
        "This is a message",
        "This is a message"
      ])
    ensure
      subscriber_a.stop
      subscriber_b.stop
    end

    it "subscribe with lock" do
      subscriber_a.subscribe("per_service_message", lock: true)
      subscriber_b.subscribe("per_service_message", lock: true)

      subscriber_a.start
      subscriber_b.start

      sleep(0.1) # Wait for subscription

      redis.publish("per_service_message", Verse::Redis::Stream::Message.new("yet another message").pack)

      # Only one subscriber received the message
      queue.pop

      sleep(0.05)
      expect(queue.size).to eq(0)
      expect(@messages["per_service_message"].map(&:content)).to eq([
        "yet another message"
      ])
    ensure
      subscriber_a.stop
      subscriber_b.stop
    end

    it "stress test" do

      subs = 10.times.map do |i|
        described_class.new(
          redis: Redis.new,
          manager: nil,
          service_name: "test:service",
          service_id: "consumer_#{i}",
        ) do |channel, message|
          (@messages[channel] ||= []) << message
          queue << message
        end
      end

      subs.each do |s|
        s.subscribe("stress_test", lock: true)
        s.start
      end

      sleep(0.05) # Wait for subscriptions to start

      random_set = [ *("A".."Z").to_a, " ", *("0".."9").to_a]

      100.times do |i|
        content = 2048.times.map{ random_set.sample }.join
        redis.publish("stress_test", Verse::Redis::Stream::Message.new(content).pack )
      end

      100.times{ queue.pop }

      expect(@messages["stress_test"].size).to eq(100)
    ensure
      subscriber_a.stop
      subscriber_b.stop
    end

  end


end