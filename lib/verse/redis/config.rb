# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    Config = Struct.new(:max_connections, :url)

    class Config
      Schema = Verse::Schema.define do
        field(:max_connections, Integer).default(1)
        field(:url, String).filled

        transform { |schema| Config.new(**schema) }
      end
    end
  end
end
