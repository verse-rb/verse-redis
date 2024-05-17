class PeriodicExposition < Verse::Exposition::Base
  def initialize
    super
  end

  # every 5 minutes
  expose on_schedule("*/5 * * * *")
  def call_once
    binding.pry
    puts "Periodic exposition"
  end
end