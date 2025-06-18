# frozen_string_literal: true

require_relative "redis/version"
require_relative "redis/plugin"

require_relative "redis/stream/event_manager"
require_relative "redis/kv_store"
require_relative "redis/lock"

require_relative "./ext"

module Verse
  # Verse::Redis is a Verse plugin that provides a Redis
  # connection pool and a Redis-backed periodic task manager.
  module Redis
  end
end
