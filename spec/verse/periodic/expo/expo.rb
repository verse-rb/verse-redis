# frozen_string_literal: true

class PeriodicExposition < Verse::Exposition::Base
  @queue = Queue.new

  class << self
    attr_reader :queue
  end

  # every 5 minutes
  expose on_schedule("*/5 * * * *")
  def call_on_cron
    PeriodicExposition.queue << :call_on_cron
  end

  # every second
  expose on_every(1, :second)
  def call_on_every
    PeriodicExposition.queue << :call_on_every
  end

  # once
  expose one_time(delay: 0.5)
  def call_one_time
    PeriodicExposition.queue << :call_one_time
  end
end
