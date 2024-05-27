# frozen_string_literal: true


class BigDecimal
  # :nodoc:
  # Monkey patching for serialization purposes.
  def to_msgpack(*args, **opts)
    to_f.to_msgpack(*args, **opts)
  end
end

require "msgpack"

# Register the Time type for MessagePack
MessagePack::DefaultFactory.register_type(
  MessagePack::Timestamp::TYPE, # or just -1
  Time,
  packer: MessagePack::Time::Packer,
  unpacker: MessagePack::Time::Unpacker
)

MessagePack::DefaultFactory.register_type(
  0x50,
  Date,
  packer: proc { |date| date.to_time.to_i.to_msgpack },
  unpacker: proc { |time| Time.at(MessagePack.unpack(time)).to_date }
)
