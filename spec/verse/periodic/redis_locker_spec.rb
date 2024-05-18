# frozen_string_literal: true

require "redis"

RSpec.describe Verse::Periodic::RedisLocker do
  before do
    Redis.new.flushall
  end

  subject(:locker1) do
    Verse::Periodic::RedisLocker.new(
      service_name: "verse_spec",
      service_id: "1", # random ID
      redis: Redis.new
    )
  end

  subject(:locker2) do
    Verse::Periodic::RedisLocker.new(
      service_name: "verse_spec",
      service_id: "2", # random ID
      redis: Redis.new
    )
  end

  context "#lock" do
    it "can lock and unlock a key" do
      lock_acquired = false

      locker1.lock("test", 4321) do
        lock_acquired = true
      end

      expect(lock_acquired).to be true
    end

    it "can't lock a key if it's already locked" do
      lock_acquired = false

      locker1.lock("test", 4321) do
        locker2.lock("test", 4321) do
          lock_acquired = true
        end
      end

      expect(lock_acquired).to be false
    end
  end
end
