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
        :stream,
        keyword_init: true
      )

      StreamConfig = Struct.new(
        :maxlen, keyword_init: true
      )

      class Config
        Schema = Verse::Schema.define do
          stream_config = Verse::Schema.define do
            field(:maxlen, Integer).default(1_000_000)
            transform { |schema| StreamConfig.new(**schema) }
          end

          field(:streams, Hash, of: stream_config)
            .default({})

          field(:maxlen, Integer).default(1_000_000)
          field(:partitions, Integer).default(16)

          field(:plugin_name, Symbol).default(:redis)

          field(:stream, Subscriber::Stream::ConfigSchema).default({})

          transform { |schema| Config.new(**schema) }
        end
      end
    end
  end
end