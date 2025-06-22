# frozen_string_literal: true

require "verse/redis/stream/subscriber/stream"
require "redis"

RSpec.describe Verse::Redis::Stream::Subscriber::Stream do
  let(:config) do
    {
      max_block_time: 1,
      min_block_time: 0.1
    }
  end

  let(:redis) { Redis.new }
  let(:redis_listener) { Redis.new }

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

  let(:queue) { Queue.new }

  subject do
    described_class.new(
      config,
      manager: nil,
      consumer_name: "test_group",
      consumer_id: "test_id",
      prefix: "VERSE:STREAM:",
      redis: redis_listener
    ) do |_channel, message|
      (@messages[message.channel] ||= []) << message
      queue << message
    end
  end

  context "#run_script" do
    it "runs LOCK and UNLOCK SCRIPTS" do
      redis.set("{VERSE:STREAM:SHARDLOCK}:SERVICE_LIVENESS:test_id", 1, ex: 30)
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq(
        [
          "test_channel", 0xffff
        ]
      )

      # all locked
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq(
        [
          "test_channel", 0x0
        ]
      )

      # reset
      redis.flushall

      # create manually a lock
      redis.set("{VERSE:STREAM:SHARDLOCK}:test_channel:1:test_group", "another_id")

      # tell that the other service is alive:
      redis.set("{VERSE:STREAM:SHARDLOCK}:SERVICE_LIVENESS:another_id", 1, ex: 30)

      # try to lock
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq(
        [
          "test_channel", (0xffff - 2)
        ]
      )

      expect(redis.get("{VERSE:STREAM:SHARDLOCK}:test_channel:2:test_group")).to eq("test_id")

      # ok now let's unlock all:
      subject.release_locks(["test_channel", 0xffff], redis)

      expect(redis.get("{VERSE:STREAM:SHARDLOCK}:test_channel:2:test_group")).to eq(nil)
      # it doesn't unlock the `another_id`
      expect(redis.get("{VERSE:STREAM:SHARDLOCK}:test_channel:1:test_group")).to eq("another_id")
    end

    it "works even if the script has been flushed (e.g. redis restarted)" do
      subject.acquire_locks(["test_channel"], redis)

      redis.script(:flush)
      redis.flushall #  Doesn't matter, we check that it has no error on evalsha

      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq(
        [
          "test_channel", 0xffff
        ]
      )
    end
  end

  context "#run" do
    it "collect messages from the stream" do
      subject.subscribe("VERSE:STREAM:test_channel")
      subject.start

      # ensure the consumers are created as they start consuming
      # events appearing after creation.
      sleep 0.1

      # Works with no-sharding
      msg = Verse::Redis::Stream::Message.new({ a: 1 })
      redis.xadd("VERSE:STREAM:test_channel$1", { msg: msg.pack })

      queue.pop
      # The message should be in the business channel "test_channel" now, not the full Redis channel
      expect(@messages["test_channel"].map(&:content)).to eq([{ a: 1 }])
    end
  end

  context "#process_messages_from_channel" do
    it "correctly extracts the business channel from a sharded channel name" do
      channel = "VERSE:STREAM:test_channel$1"
      messages = [
        ["1625071342000-0", { "msg" => "message1" }]
      ]
      channel_messages = [channel, messages]

      unpacked_message = Verse::Redis::Stream::Message.new({ a: 1 }, channel: "test_channel")

      expect(Verse::Redis::Stream::Message).to receive(:unpack).with(
        subject,
        "message1",
        channel: "test_channel",
        consumer_group: "test_group"
      ).and_return(unpacked_message)

      expect(subject).to receive(:process_message).with(channel, unpacked_message)

      subject.send(:process_messages_from_channel, channel_messages)
    end
  end
end
