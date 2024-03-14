# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    Config = Struct.new(:max_connections, :url, keyword_init: true)

    class Config
      Schema = Verse::Schema.define do
        field(:max_connections, Integer).default(1).rule("must be positive integer", &:positive?)
        field(:url, String).filled.default("redis://localhost:6379")

        transform { |schema| Config.new(**schema) }
      end
    end
  end
end
