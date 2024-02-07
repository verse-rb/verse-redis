require "verse/redis/stream/subscriber"
require "redis"

RSpec.describe Verse::Redis::Stream::Subscriber do
  let(:config) {
    {
      max_block_time: 1,
      min_block_time: 0.1,
    }
  }

  let(:redis) {
    Redis.new
  }

  before {
    redis.flushall
    @messages = []
  }

  let(:redis_block) {
    -> (&block) { block.call(redis) }
  }

  subject {
    described_class.new(config,
      consumer_name: "test_group",
      consumer_id: "test_id",
      redis: redis,
    ) do |message|
      @messages << message
    end
  }

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
      redis.set("VERSE:STREAM:SHARDLOCK:test_channel:1:test_group", "another_id")

      #try to lock
      acquired = subject.acquire_locks(["test_channel"], redis)
      expect(acquired).to eq([
        "test_channel", (0xffff - 2)
      ])

      expect(redis.get("VERSE:STREAM:SHARDLOCK:test_channel:2:test_group")).to eq("test_id")

      # ok now let's unlock all:
      subject.release_locks(["test_channel", 0xffff], redis)

      expect(redis.get("VERSE:STREAM:SHARDLOCK:test_channel:2:test_group")).to eq(nil)
      # it doesn't unlock the `another_id`
      expect(redis.get("VERSE:STREAM:SHARDLOCK:test_channel:1:test_group")).to eq("another_id")
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
  end


end