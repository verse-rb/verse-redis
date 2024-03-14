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
    it "can publish and receive a message (MODE_CONSUMER)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe("example:topic", mode: Verse::Event::Manager::MODE_CONSUMER) do |message, channel|
          @queue.push(message)
          total_events += 1
        end
      end

      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      em2 = Verse::Redis::Stream::EventManager.new(
        service_name: "verse_spec",
        service_id: "1234", # random ID
        config: {},
        logger: Verse.logger
      )

      em2.subscribe("example:topic", mode: Verse::Event::Manager::MODE_CONSUMER) do |message, channel|
        # Creating another one to deal with concurrency with consumers
        @queue.push(message)
        total_events += 1
      end

      Verse.on_stop do
        em2.stop
      end

      em2.start

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

    it "can publish and receive a message (MODE_BROADCAST)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe("example:topic", mode: Verse::Event::Manager::MODE_BROADCAST) do |message, channel|
          @queue.push(message)
          total_events += 1
        end
      end

      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      em2 = Verse::Redis::Stream::EventManager.new(
        service_name: "verse_spec",
        service_id: "1234", # random ID
        config: {},
        logger: Verse.logger
      )

      em2.subscribe("example:topic", mode: Verse::Event::Manager::MODE_BROADCAST) do |message, channel|
        # Creating another one to deal with concurrency with consumers
        @queue.push(message)
        total_events += 1
      end

      Verse.on_stop do
        em2.stop
      end

      em2.start

      5.times do
        Verse.publish(
          "example:topic",
          "This is a payload",
          headers: { header1: "value1"}
        )
      end

      10.times do
        queue.pop
      end

      # Received each event only once.
      expect(total_events).to eq(10)
    end

    it "can publish and receive a message (MODE_COMMAND)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe("example:topic", mode: Verse::Event::Manager::MODE_COMMAND) do |message, channel|
          @queue.push(message)
          total_events += 1
        end
      end

      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      em2 = Verse::Redis::Stream::EventManager.new(
        service_name: "verse_spec",
        service_id: "1234", # random ID
        config: {},
        logger: Verse.logger
      )

      em2.subscribe("example:topic", mode: Verse::Event::Manager::MODE_COMMAND) do |message, channel|
        # Creating another one to deal with concurrency with consumers
        @queue.push(message)
        total_events += 1
      end

      Verse.on_stop do
        em2.stop
      end

      em2.start

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

    it "can publish resource event" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe_resource_event(
          resource_type: "example",
          event: "topic"
        ) do |message, channel|
          @queue.push(message)
          total_events += 1
        end
      end

      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      em2 = Verse::Redis::Stream::EventManager.new(
        service_name: "verse_spec",
        service_id: "1234", # random ID
        config: {},
        logger: Verse.logger
      )

      em2.subscribe_resource_event(
        resource_type: "example",
        event: "topic"
      ) do |message, channel|
        # Creating another one to deal with concurrency with consumers
        @queue.push(message)
        total_events += 1
      end

      Verse.on_stop do
        em2.stop
      end

      em2.start

      5.times do |x|
        Verse.publish_resource_event(
          resource_type: "example",
          resource_id: x,
          event: "topic",
          payload: "This is a payload",
          headers: { header1: "value1"}
        )
      end

      5.times do
        queue.pop
      end

      # Received each event only once.
      expect(total_events).to eq(5)
    end

  end

end
