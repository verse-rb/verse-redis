# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    KVConfigSchema = Verse::Schema.define do
      field(:plugin, Symbol).default(:redis)
      field(:key_prefix, [NilClass, String]).default(nil)
    end

    KVConfig = KVConfigSchema.dataclass
  end
end
