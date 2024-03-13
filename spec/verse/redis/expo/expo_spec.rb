require "redis"
require_relative "./expo"

RSpec.describe Verse::Exposition::Base do

  before do
    ::Redis.new.flushall
    Verse.on_boot { MyExposition.register }

    Verse.start(
      :test,
      config_path: "./spec/spec_data/config.yml"
    )

    MyExposition.clear_log
  end

  after do
    Verse.stop
  end

  let(:redis) { Verse::Plugin[:redis] }

  it "can register and fire resource events" do
    expect(MyExposition.log.empty?).to be true

    10.times do |x|
      Verse.publish_resource_event(
          resource_type: "resource_type",
          resource_id: x,
          event: "event",
          payload: "This is a payload",
          headers: { header1: "value1"}
        )
    end

    sleep 0.01 # Wait to receive all.

    expect(MyExposition.log.size).to eq(10)
  end

end
