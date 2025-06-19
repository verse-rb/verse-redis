# frozen_string_literal: true

require "spec_helper"
require "verse/redis/cache_adapter"
require "redis"

RSpec.describe Verse::Redis::CacheAdapter do
  let(:redis) { Redis.new }
  let(:plugin) { double("plugin") }

  before do
    allow(Verse::Plugin).to receive(:[]).with("verse-redis").and_return(plugin)
    allow(plugin).to receive(:with_client).and_yield(redis)
    redis.flushdb
  end

  let(:cache) { described_class.new(config) }
  let(:config) { {} }

  describe "#fetch" do
    it "gets a value from redis" do
      redis.set("verse:cache:foo:bar", "baz")
      expect(cache.fetch("foo", "bar")).to eq("baz")
    end
  end

  describe "#cache" do
    it "sets a value in redis" do
      cache.cache("foo", "bar", "baz")
      expect(redis.get("verse:cache:foo:bar")).to eq("baz")
    end

    it "sets a value with a ttl" do
      cache.cache("foo", "bar", "baz", ex: 10)
      expect(redis.ttl("verse:cache:foo:bar")).to be_within(1).of(10)
    end
  end

  describe "#remove" do
    it "deletes a value from redis" do
      redis.set("verse:cache:foo:bar", "baz")
      cache.remove("foo", "bar")
      expect(redis.get("verse:cache:foo:bar")).to be_nil
    end
  end

  describe "#flush" do
    it "clears all values from redis" do
      redis.set("verse:cache:foo:bar", "baz")
      redis.set("verse:cache:foo:qux", "quux")
      cache.flush("foo", ["*"])
      expect(redis.keys("verse:cache:foo:*")).to be_empty
    end

    it "clears selected values from redis" do
      redis.set("verse:cache:foo:bar", "baz")
      redis.set("verse:cache:foo:qux", "quux")
      cache.flush("foo", ["bar"])
      expect(redis.get("verse:cache:foo:bar")).to be_nil
      expect(redis.get("verse:cache:foo:qux")).to eq("quux")
    end
  end

  context "with key_prefix" do
    let(:config) { { key_prefix: "my_prefix" } }

    describe "#fetch" do
      it "gets a value from redis" do
        redis.set("my_prefix:foo:bar", "baz")
        expect(cache.fetch("foo", "bar")).to eq("baz")
      end
    end

    describe "#cache" do
      it "sets a value in redis" do
        cache.cache("foo", "bar", "baz")
        expect(redis.get("my_prefix:foo:bar")).to eq("baz")
      end

      it "sets a value with a ttl" do
        cache.cache("foo", "bar", "baz", ex: 10)
        expect(redis.ttl("my_prefix:foo:bar")).to be_within(1).of(10)
      end
    end

    describe "#remove" do
      it "deletes a value from redis" do
        redis.set("my_prefix:foo:bar", "baz")
        cache.remove("foo", "bar")
        expect(redis.get("my_prefix:foo:bar")).to be_nil
      end
    end

    describe "#flush" do
      it "clears all values from redis with the prefix" do
        redis.set("my_prefix:foo:bar", "baz")
        redis.set("my_prefix:foo:qux", "quux")
        redis.set("other:foo:bar", "baz")
        cache.flush("foo", ["*"])
        expect(redis.keys("my_prefix:foo:*")).to be_empty
        expect(redis.get("other:foo:bar")).to eq("baz")
      end
    end
  end
end
