# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    CacheConfig = Verse::Schema.define do
      field(:plugin, String).default("verse-redis")
      field(:key_prefix, String).default("verse:cache")
    end.dataclass
  end
end
