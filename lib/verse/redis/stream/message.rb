require "zlib"
require "msgpack"

module Verse
  module Redis
    module Stream
      # A naive implementation of a message for Redis Streams
      # using msgpack and zlib to compress the message.
      class Message < Verse::Event::Message
        attr_reader :id, :stream, :consumer_group

        def initialize(
          manager,
          content,
          headers: {},
          reply_to: nil,
          id: nil,
          stream: nil,
          consumer_group: nil
        )
          @id     = id
          @stream = stream
          @consumer_group = consumer_group

          super(manager, content, headers: headers, reply_to: reply_to)
        end

        def pack
          Zlib::Deflate.deflate(
            {
              content: @content,
              headers: @headers,
              reply_to: @reply_to
            }.to_msgpack
          )
        end

        def self.unpack(manager, data)
          hash = MessagePack.unpack(Zlib::Inflate.inflate(data))
          new(manager, hash[:content], headers: hash[:headers], reply_to: hash[:reply_to])
        end

        def ack
          unless @id && @stream && @consumer_group
            raise "Cannot ack message without id, stream and consumer_group"
          end

          manager.with_redis{ |rd| rd.xack(@stream, @consumer_group, @id) }
        end

      end
    end
  end
end
