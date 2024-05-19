# frozen_string_literal: true

require "verse/schema"

module Verse
  module Periodic
    Config = Struct.new(:locker_class, :locker_config, keyword_init: true)

    class Config
      Schema = Verse::Schema.define do
        field?(:locker_class, String).filled.default("Verse::Periodic::Locker::None")
        field?(:locker_config, Hash).default({})

        transform { |schema| Config.new(**schema) }
      end
    end
  end
end
