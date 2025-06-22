# frozen_string_literal: true

require "spec_helper"
require "verse/redis/lock"
require "redis"

RSpec.describe Verse::Redis::Lock do
  let(:redis) { Redis.new }
  let(:plugin) { double("plugin") }

  before do
    allow(Verse::Plugin).to receive(:[]).with(:redis).and_return(plugin)
    allow(plugin).to receive(:with_client).and_yield(redis)
    redis.flushdb
  end

  let(:lock) { described_class.new(config) }
  let(:config) { {} }

  describe "#acquire" do
    it "acquires a lock" do
      token = lock.acquire("foo", 1000, 0)
      expect(token).to be_a(String)
      expect(redis.get("foo")).to eq(token)
    end

    it "fails to acquire a locked lock" do
      lock.acquire("foo", 1000, 0)
      expect(lock.acquire("foo", 1000, 0)).to be_nil
    end

    it "acquires a lock after it is released" do
      token = lock.acquire("foo", 1000, 0)
      lock.release("foo", token)
      expect(lock.acquire("foo", 1000, 0)).to be_a(String)
    end

    it "waits to acquire a lock" do
      token = lock.acquire("foo", 100, 0)
      thread = Thread.new do
        sleep 0.05
        lock.release("foo", token)
      end
      expect(lock.acquire("foo", 1000, 200)).to be_a(String)
      thread.join
    end
  end

  describe "#release" do
    it "releases a lock" do
      token = lock.acquire("foo", 1000, 0)
      expect(lock.release("foo", token)).to be true
      expect(redis.get("foo")).to be_nil
    end

    it "fails to release a lock with a wrong token" do
      lock.acquire("foo", 1000, 0)
      expect(lock.release("foo", "wrong_token")).to be false
    end
  end

  describe "#renew" do
    it "renews a lock" do
      token = lock.acquire("foo", 100, 0)
      expect(lock.renew("foo", token, 1000)).to be true
      expect(redis.pttl("foo")).to be_within(100).of(1000)
    end

    it "fails to renew a lock with a wrong token" do
      lock.acquire("foo", 100, 0)
      expect(lock.renew("foo", "wrong_token", 1000)).to be false
    end
  end

  context "with key_prefix" do
    let(:config) { { key_prefix: "my_prefix" } }

    it "uses the prefix" do
      lock.acquire("foo", 1000, 0)
      expect(redis.keys("my_prefix:*")).to eq(["my_prefix:foo"])
    end
  end
end
