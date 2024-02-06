RSpec.describe Verse::Redis::Config do

  it "valid and invalid configs" do
    [
      [{
        max_connections: 1,
        url: "redis://localhost:6379"
      }, true],
      [{}, false]
    ].each do |(config, valid)|
      expect(Verse::Redis::Config::Schema.validate(config).success?).to be(valid)
    end
  end

end