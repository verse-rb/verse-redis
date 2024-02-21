# frozen_string_literal: true

RSpec.describe Verse::Periodic::Executioner do
  before do
    @executioner = Verse::Periodic::Executioner.new
  end

  after do
    @executioner.stop
  end

  describe "#call" do
    it "call multiple times" do
      x = 1

      100.times do
        @executioner.call(lambda do
          x += 1
        end)
      end

      sleep 0.01 until @executioner.empty?

      expect(x).to eq(101)
    end

    it "logs error" do
      expect(Verse.logger).to receive(:warn).with("Error in periodic task:")
      expect(Verse.logger).to receive(:warn).with(StandardError)

      @executioner.call(-> { raise StandardError })
      sleep 0.01 until @executioner.empty?
    end
  end
end
