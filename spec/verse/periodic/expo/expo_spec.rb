# frozen_string_literal: true

require "redis"

RSpec.describe Verse::Exposition::Base do
  it "should run the exposition" do
    begin
      # This should run the cron job asap.
      now = Time.new(2021, 1, 1, 0, 4, 59.9)
      now_after = now + 1 # one second later.

      puts "NOW = #{now.to_f}"

      allow(Time).to receive(:now).and_return(now)

      ::Redis.new.flushall

      Verse.on_boot {
        require_relative "./expo"
        PeriodicExposition.register
      }

      Verse.start(
        :test,
        config_path: "./spec/spec_data/config_periodic.yml"
      )

      allow(Time).to receive(:now).and_return(now_after)

      events = 3.times.map{ PeriodicExposition.queue.pop }
      expect(events.sort).to eq(
        %i[call_on_cron call_on_every call_one_time]
      )
    ensure
      Verse.stop
    end
  end
end
