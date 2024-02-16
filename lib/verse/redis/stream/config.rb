# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    module Stream
      Config = Struct.new(
        :streams,
        :maxlen,
        :partitions,
        :plugin_name,
        keyword_init: true
      )

      StreamConfig = Struct.new(
        :partitions, :maxlen
      )

      class Config
        Schema = Verse::Schema.define do
          field(:streams, Hash).default([]) do
            field(:partitions, Integer).default(16)
            field(:maxlen, Integer).default(1_000_000)
          end.transform { |h| h.transform_values{ |v| StreamConfig.new(**x) } }

          field(:maxlen, Integer).default(1_000_000)
          field(:partitions, Integer).default(16)

          field(:plugin_name, String).default("redis")

          transform { |schema| Config.new(**schema) }
        end
      end
    end
  end
end