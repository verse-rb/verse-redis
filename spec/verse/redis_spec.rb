# frozen_string_literal: true

RSpec.describe Verse::Redis do
  it "has a version number" do
    expect(Verse::Redis::VERSION).not_to be nil
  end
end
