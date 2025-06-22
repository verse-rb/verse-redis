# frozen_string_literal: true

require "spec_helper"
require "verse/redis/counter"
require "redis"

RSpec.describe Verse::Redis::Counter do
  let(:redis) { Redis.new }
  let(:plugin) { double("plugin") }

  before do
    allow(Verse::Plugin).to receive(:[]).with(:redis).and_return(plugin)
    allow(plugin).to receive(:with_client).and_yield(redis)
    redis.flushdb
  end

  let(:counter) { described_class.new(config) }
  let(:config) { {} }

  describe "#increment" do
    it "increments a counter" do
      expect(counter.increment("foo")).to eq(1)
      expect(counter.increment("foo")).to eq(2)
    end

    it "increments by a given amount" do
      expect(counter.increment("foo", 5)).to eq(5)
    end

    it "sets a ttl" do
      counter.increment("foo", 1, ttl: 10)
      expect(redis.ttl("foo")).to be_within(1).of(10)
    end
  end

  describe "#decrement" do
    it "decrements a counter" do
      counter.increment("foo", 5)
      expect(counter.decrement("foo")).to eq(4)
    end
  end

  describe "#get" do
    it "gets a counter value" do
      counter.set("foo", 10)
      expect(counter.get("foo")).to eq(10)
    end
  end

  describe "#set" do
    it "sets a counter value" do
      counter.set("foo", 10)
      expect(redis.get("foo")).to eq("10")
    end

    it "sets a counter value with a ttl" do
      counter.set("foo", 10, ttl: 10)
      expect(redis.ttl("foo")).to be_within(1).of(10)
    end
  end

  describe "#delete" do
    it "deletes a counter" do
      counter.set("foo", 10)
      counter.delete("foo")
      expect(redis.get("foo")).to be_nil
    end
  end

  describe "#exists?" do
    it "checks if a counter exists" do
      expect(counter.exists?("foo")).to be false
      counter.set("foo", 10)
      expect(counter.exists?("foo")).to be true
    end
  end

  context "with key_prefix" do
    let(:config) { { key_prefix: "my_prefix" } }

    it "uses the prefix" do
      counter.increment("foo")
      expect(redis.keys("my_prefix:*")).to eq(["my_prefix:foo"])
    end
  end
end
