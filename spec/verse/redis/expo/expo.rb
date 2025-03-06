# frozen_string_literal: true

class MyExposition < Verse::Exposition::Base
  @log = []
  @channels = []

  class << self
    attr_reader :log, :channels

    def clear_log
      @log = []
      @channels = []
    end
  end

  expose on_resource_event("resource_type", "event")
  def resource_event_fired
    auth_context.mark_as_checked!
    self.class.log << "resource_event_fired"
    self.class.channels << message.channel
  end

  expose on_event("some:channel")
  def some_channel_event_fired
    auth_context.mark_as_checked!
    self.class.log << "some_channel_event_fired"
    self.class.channels << message.channel
  end
end
