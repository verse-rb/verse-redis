# frozen_string_literal: true

require "spec_helper"
require "verse/redis/kv_store"
require "redis"

RSpec.describe Verse::Redis::KVStore do
  let(:redis) { Redis.new }
  let(:plugin) { double("plugin") }

  before do
    allow(Verse::Plugin).to receive(:[]).with(:redis).and_return(plugin)
    allow(plugin).to receive(:with_client).and_yield(redis)
    redis.flushdb
  end

  let(:kv_store) { described_class.new(config) }
  let(:config) { {} }

  describe "#get" do
    it "gets a value from redis" do
      redis.set("foo", "bar")
      expect(kv_store.get("foo")).to eq("bar")
    end
  end

  describe "#set" do
    it "sets a value in redis" do
      kv_store.set("foo", "bar")
      expect(redis.get("foo")).to eq("bar")
    end

    it "sets a value with a ttl" do
      kv_store.set("foo", "bar", ttl: 10)
      expect(redis.ttl("foo")).to be_within(1).of(10)
    end
  end

  describe "#delete" do
    it "deletes a value from redis" do
      redis.set("foo", "bar")
      kv_store.delete("foo")
      expect(redis.get("foo")).to be_nil
    end
  end

  describe "#clear_all" do
    it "clears all values from redis" do
      redis.set("foo", "bar")
      redis.set("baz", "qux")
      kv_store.clear_all
      expect(redis.keys("*")).to be_empty
    end
  end

  context "with key_prefix" do
    let(:config) { { key_prefix: "my_prefix" } }

    context "with nil key_prefix" do
      let(:config) { { key_prefix: nil } }

      it "doesn't add prefix" do
        kv_store.set("foo", "bar")
        expect(redis.get("foo")).to eq("bar")
      end
    end

    context "with empty key_prefix" do
      let(:config) { { key_prefix: "" } }

      it "doesn't add prefix" do
        kv_store.set("foo", "bar")
        expect(redis.get("foo")).to eq("bar")
      end
    end

    describe "#get" do
      it "gets a value from redis" do
        redis.set("my_prefix:foo", "bar")
        expect(kv_store.get("foo")).to eq("bar")
      end
    end

    describe "#set" do
      it "sets a value in redis" do
        kv_store.set("foo", "bar")
        expect(redis.get("my_prefix:foo")).to eq("bar")
      end

      it "sets a value with a ttl" do
        kv_store.set("foo", "bar", ttl: 10)
        expect(redis.ttl("my_prefix:foo")).to be_within(1).of(10)
      end
    end

    describe "#delete" do
      it "deletes a value from redis" do
        redis.set("my_prefix:foo", "bar")
        kv_store.delete("foo")
        expect(redis.get("my_prefix:foo")).to be_nil
      end
    end

    describe "#clear_all" do
      it "clears all values from redis with the prefix" do
        redis.set("my_prefix:foo", "bar")
        redis.set("my_prefix:baz", "qux")
        redis.set("other:foo", "bar")
        kv_store.clear_all
        expect(redis.keys("my_prefix:*")).to be_empty
        expect(redis.get("other:foo")).to eq("bar")
      end
    end
  end
end
