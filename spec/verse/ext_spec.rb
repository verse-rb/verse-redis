require_relative "../../lib/verse/ext"

RSpec.describe "Extensions" do
  it "should monkey patch BigDecimal" do
    bd = MessagePack.unpack(BigDecimal(1).to_msgpack)
    expect(bd).to eq(1.0)
  end

  it "should register Time type for MessagePack" do
    time = Time.now
    expect(MessagePack.unpack(MessagePack.pack(time))).to eq(time)
  end

  it "should register Date type for MessagePack" do
    date = Date.today
    expect(MessagePack.unpack(MessagePack.pack(date))).to eq(date)
  end

end