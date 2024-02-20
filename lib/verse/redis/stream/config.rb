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
        :partitions, :maxlen, keyword_init: true
      )

      class Config
        Schema = Verse::Schema.define do
          stream_config = Verse::Schema.define do
            field(:partitions, Integer).default(16)
            field(:maxlen, Integer).default(1_000_000)
          end

          field(:streams, Hash, of: stream_config)
            .default({})
            .transform { |h| h.transform_values{ |v| StreamConfig.new(**v) } }

          field(:maxlen, Integer).default(1_000_000)
          field(:partitions, Integer).default(16)

          field(:plugin_name, Symbol).default(:redis)

          transform { |schema| Config.new(**schema) }
        end
      end
    end
  end
end