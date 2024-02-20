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

  subject(:subscriber_a) do
    described_class.new(
      redis: Redis.new,
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
      service_name: "test:service",
      service_id: "b",
    ) do |channel, message|
      (@messages[channel] ||= []) << message
      queue << message
    end
  end

  context "publish/subscribe" do
    it "Receive broadcast messages" do
      subscriber_a.listen_channel("test_channel", lock: false)
      subscriber_b.listen_channel("test_channel", lock: false)

      subscriber_a.start
      subscriber_b.start

      sleep(0.1) # Wait for subscription

      redis.publish("test_channel", "This is a message")

      # Received for each subscriber:
      queue.pop
      queue.pop

      expect(@messages["test_channel"]).to eq([
        "This is a message",
        "This is a message"
      ])
    ensure
      subscriber_a.stop
      subscriber_b.stop
    end

    it "subscribe with lock" do
      subscriber_a.listen_channel("per_service_message", lock: true)
      subscriber_b.listen_channel("per_service_message", lock: true)

      subscriber_a.start
      subscriber_b.start

      sleep(0.1) # Wait for subscription

      redis.publish("per_service_message", "yet another message")

      # Only one subscriber received the message
      queue.pop

      sleep(0.05)
      expect(queue.size).to eq(0)
      expect(@messages["per_service_message"]).to eq([
        "yet another message"
      ])
    ensure
      subscriber_a.stop
      subscriber_b.stop
    end
  end


end