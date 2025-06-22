# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    LockConfigSchema = Verse::Schema.define do
      field(:plugin, Symbol).default(:redis)
      field(:key_prefix, [NilClass, String]).default(nil)
    end

    LockConfig = LockConfigSchema.dataclass
  end
end
