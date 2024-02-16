# frozen_string_literal: true

RSpec.describe Verse::Redis::Stream::EventManager do
  before do
    Verse.start(
      :test,
      config_path: "./spec/spec_data/config.yml"
    )

    Verse::Plugin[:redis].with_client do |redis|
      redis.flushall
    end
  end

  context "#publish" do
    it "can publish a message" do

    end
  end
end
