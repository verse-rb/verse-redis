# frozen_string_literal: true

RSpec.describe Verse::Redis::Stream::EventManager do
  before do
    @queue = Queue.new

    Verse.on_boot do
      Verse::Plugin[:redis].with_client do |redis|
        redis.flushall
      end
    end
  end

  after do
    Verse.stop
  end

  let(:queue){ @queue }

  context "#publish" do
    it "can publish and receive a message (mode consumer)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe("example:topic", Verse::Event::Manager::MODE_CONSUMER) do |message, channel|
          @queue.push(message)
          total_events += 1
        end
      end

      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      puts "plugin = #{Verse::Plugin[:redis]}"

      em2 = Verse::Redis::Stream::EventManager.new(
        service_name: "verse_spec",
        service_id: "1234", # random ID
        config: {},
        logger: Verse.logger
      )

      puts "SUBSCRIBE 2"
      em2.subscribe("example:topic", Verse::Event::Manager::MODE_CONSUMER) do |message, channel|
        # Creating another one to deal with concurrency with consumers
        @queue.push(message)
        total_events += 1
      end

      Verse.on_stop do
        em2.stop
      end

      em2.start

      sleep 0.05 # be sure that the threads have created the consumers

      5.times do
        Verse.publish(
          "example:topic",
          "This is a payload",
          headers: { header1: "value1"}
        )
      end

      5.times do
        queue.pop
      end

      # Received each event only once.
      expect(total_events).to eq(5)
    end

    it "can publish and receive a message (mode broadcast)" do
      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      Verse.on_boot do
        Verse.event_manager.subscribe("example:topic", Verse::Event::Manager::MODE_CONSUMER) do |message, channel|
          @queue.push(message)
        end
      end

      sleep 0.05

      5.times do
        Verse.publish(
          "example:topic",
          "This is a payload",
          headers: { header1: "value1"}
        )
      end

      5.times do
        queue.pop
      end

    end

  end

end
