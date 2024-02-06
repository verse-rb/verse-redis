# frozen_string_literal: true

require "verse/schema"

module Verse
  module Redis
    module Stream
      Config = Struct.new(
        :streams
        :plugin_name
      )

      StreamConfig = Struct.new(
        :name,
        :partitions,
        :maxlen
      )

      class Config
        Schema = Verse::Schema.define do
          field(:streams, Array).default([]) do
            field(:name, String).filled
            field(:partitions, Integer).default(16)
            field(:maxlen, Integer).default(1_000_000)
          end.transform { |arr| arr.map { |x| [x[:name], StreamConfig.new(**x)] }.to_h }

          field(:plugin_name, String).default("redis")

          transform { |schema| Config.new(**schema) }
        end
      end
    end
  end
end