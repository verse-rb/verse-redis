# frozen_string_literal: true

require "zlib"
require "msgpack"

module Verse
  module Redis
    module Stream
      # A naive implementation of a message for Redis Streams
      # using msgpack and zlib to compress the message.
      class Message < Verse::Event::Message
        attr_reader :id, :consumer_group, :redis_channel

        def initialize(
          content,
          headers: {},
          manager: nil,
          reply_to: nil,
          id: nil,
          channel: nil,
          consumer_group: nil
        )
          @id = id || SecureRandom.random_number(2 << 48).to_s(36)
          @consumer_group = consumer_group

          super(content, manager:, headers:, reply_to:, channel:)
        end
        # rubocop:enable Metrics/ParameterLists

        def pack
          Zlib::Deflate.deflate(
            {
              i: @id,
              c: @content,
              h: @headers,
              r: @reply_to
            }.to_msgpack
          )
        end

        def self.unpack(manager, data, channel: nil, consumer_group: nil)
          hash = MessagePack.unpack(
            Zlib::Inflate.inflate(data),
            symbolize_keys: true
          )

          new(
            hash[:c],
            headers: hash[:h],
            reply_to: hash[:r],
            id: hash[:i],
            manager:,
            channel:,
            consumer_group:
          )
        end

        def ack
          raise "Cannot ack message without id, channel and consumer_group" unless @id && @channel && @consumer_group

          manager.with_redis { |rd| rd.xack(@channel, @consumer_group, @id) }
        end
      end
    end
  end
end
