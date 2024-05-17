# frozen_string_literal: true

require "redis"

RSpec.describe Verse::Exposition::Base do
  it "should run the exposition" do
    # This should run the cron job asap.
    now = Time.new(2020, 1, 1, 4, 59, 0.999)

    allow(Time).to receive(:now).and_return(now)

    ::Redis.new.flushall

    Verse.on_boot {
      require_relative "./expo"
      PeriodicExposition.register
    }

    puts "START?"

    Verse.start(
      :test,
      config_path: "./spec/spec_data/config_periodic.yml"
    )

  end

end