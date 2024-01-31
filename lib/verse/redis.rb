# frozen_string_literal: true

require_relative "redis/version"
require_relative "redis/plugin"

require_relative "periodic/executioner"
require_relative "periodic/manager"
require_relative "periodic/task"

module Verse
  # Verse::Redis is a Verse plugin that provides a Redis
  # connection pool and a Redis-backed periodic task manager.
  module Redis
  end
end
