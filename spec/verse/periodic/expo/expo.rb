class PeriodicExposition < Verse::Exposition::Base

  @queue = Queue.new

  class << self
    attr_reader :queue
  end

  # every 5 minutes
  expose on_schedule("*/5 * * * *")
  def call_on_cron
    @queue << :call_on_cron
  end

  # every second
  expose on_every(1, :second)
  def call_on_every
    @queue << :call_on_every
  end

  # once
  expose one_time(delay: 0.5)
  def call_one_time
    @queue << :call_one_time
  end
end