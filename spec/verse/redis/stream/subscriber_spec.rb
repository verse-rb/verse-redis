require "verse/redis/stream/stream_subscriber"
require "redis"

RSpec.describe Verse::Redis::Stream::StreamSubscriber do
  let(:config) {
    {
      max_block_time: 1,
      min_block_time: 0.1,
    }
  }

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
    described_class.new(config,
      consumer_name: "test_group",
      consumer_id: "test_id",
      redis: redis_listener,
    ) do |channel, message|
      (@messages[channel] ||= []) << message
      queue << message
    end
  end

  context "#run_script" do
    it "runs LOCK and UNLOCK SCRIPTS" do
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq([
        "test_channel", 0xffff
      ])

      # all locked
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq([
        "test_channel", 0x0
      ])

      #reset
      redis.flushall

      #create manually a lock
      redis.set("{VERSE:STREAM:SHARDLOCK}:test_channel:1:test_group", "another_id")

      #try to lock
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq([
        "test_channel", (0xffff - 2)
      ])

      expect(redis.get("{VERSE:STREAM:SHARDLOCK}:test_channel:2:test_group")).to eq("test_id")

      # ok now let's unlock all:
      subject.release_locks(["test_channel", 0xffff], redis)

      expect(redis.get("{VERSE:STREAM:SHARDLOCK}:test_channel:2:test_group")).to eq(nil)
      # it doesn't unlock the `another_id`
      expect(redis.get("{VERSE:STREAM:SHARDLOCK}:test_channel:1:test_group")).to eq("another_id")
    end

    it "works even if the script has been flushed (e.g. redis restarted)" do
      acquired = subject.acquire_locks(["test_channel"], redis)

      redis.script(:flush)
      redis.flushall #  Doesn't matter, we check that it has no error on evalsha

      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq([
        "test_channel", 0xffff
      ])
    end
  end

  context "#run" do
    it "collect messages from the stream" do
      subject.listen_channel("VERSE:STREAM:test_channel")
      subject.start

      # ensure the consumers are created as they start consuming
      # events appearing after creation.
      sleep 0.1

      # Works with no-sharding
      redis.xadd("VERSE:STREAM:test_channel", {a: 1})

      message = queue.pop
      expect(@messages["VERSE:STREAM:test_channel"]).to eq([{"a" => "1"}])
    end
  end


end