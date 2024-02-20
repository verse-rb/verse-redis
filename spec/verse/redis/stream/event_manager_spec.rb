# frozen_string_literal: true

RSpec.describe Verse::Redis::Stream::EventManager do
  before do
    @queue = Queue.new

    Verse.on_boot do
      Verse.event_manager.subscribe("example:topic") do |message, subject|
        @queue.push(message)
      end
    end

    Verse.start(
      :test,
      config_path: "./spec/spec_data/config.yml"
    )

    Verse::Plugin[:redis].with_client do |redis|
      redis.flushall
    end
  end

  let(:queue){ Queue.new }

  context "#publish" do
    it "can publish a message" do
      Verse.publish(
        "example:topic",
        "This is a payload",
        headers: { header1: "value1"}
      )

      message = queue.pop
    end
  end
end
