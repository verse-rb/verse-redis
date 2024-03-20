# frozen_string_literal: true

RSpec.describe Verse::Redis::Stream::EventManager do
  before do
    @queue = Queue.new

    Verse.on_boot do
      Verse::Plugin[:redis].with_client(&:flushall)
    end
  end

  after do
    Verse.stop
  end

  let(:queue) { @queue }

  context "#publish and #subscribe" do
    it "can publish and receive a message (MODE_CONSUMER)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe("example:topic",
                                      mode: Verse::Event::Manager::MODE_CONSUMER) do |message, _channel|
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

      em2.subscribe("example:topic", mode: Verse::Event::Manager::MODE_CONSUMER) do |message, _channel|
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
          headers: { header1: "value1" }
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
        Verse.event_manager.subscribe("example:topic",
                                      mode: Verse::Event::Manager::MODE_BROADCAST) do |message, _channel|
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

      em2.subscribe("example:topic", mode: Verse::Event::Manager::MODE_BROADCAST) do |message, _channel|
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
          headers: { header1: "value1" }
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
        Verse.event_manager.subscribe("example:topic", mode: Verse::Event::Manager::MODE_COMMAND) do |message, _channel|
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

      em2.subscribe("example:topic", mode: Verse::Event::Manager::MODE_COMMAND) do |message, _channel|
        # Creating another one to deal with concurrency with consumers
        total_events += 1
        @queue.push(message)
      end

      Verse.on_stop do
        em2.stop
      end

      em2.start

      5.times do
        Verse.publish(
          "example:topic",
          "This is a payload",
          headers: { header1: "value1" }
        )
      end

      5.times do
        queue.pop
      end

      # Received each event only once.
      expect(total_events).to eq(5)
    end

    it "can publish and receive resource event (mode CONSUMER)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe_resource_event(
          resource_type: "example",
          event: "topic"
        ) do |message, _channel|
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
      ) do |message, _channel|
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
          headers: { header1: "value1" }
        )
      end

      5.times do
        queue.pop
      end

      # Received each event only once.
      expect(total_events).to eq(5)
    end

    it "can publish and receive resource event (mode BROADCAST)" do
      total_events = 0

      Verse.on_boot do
        # Creating the real event manager
        Verse.event_manager.subscribe_resource_event(
          resource_type: "example",
          event: "topic",
          mode: Verse::Event::Manager::MODE_BROADCAST
        ) do |message, _channel|
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
        event: "topic",
        mode: Verse::Event::Manager::MODE_BROADCAST
      ) do |message, _channel|
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
          headers: { header1: "value1" }
        )
      end

      10.times do
        queue.pop
      end

      # Received each event twice.
      expect(total_events).to eq(10)
    end
  end

  context "#request and #request_all" do
    it "can request and receive a message" do
      Queue.new

      Verse.on_boot do
        Verse.event_manager.subscribe(
          "example:add",
          mode: Verse::Event::Manager::MODE_COMMAND
        ) do |message, _channel|
          message.reply(message.content.sum)
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

      message = em2.request("example:add", [1, 2, 3], timeout: 2000)
      expect(message.content).to eq(6)
    end

    it "can request_all and receive a set of messages (mode command)" do
      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      em_subscribers = 5.times.map do |x|
        em = Verse::Redis::Stream::EventManager.new(
          service_name: "verse_spec",
          service_id: x, # random ID
          config: {},
          logger: Verse.logger
        )

        em.subscribe(
          "example:random_number",
          mode: Verse::Event::Manager::MODE_COMMAND
        ) do |message, _channel|
          message.reply(rand(100))
        end

        em.start
        em
      end

      messages = Verse.request_all("example:random_number", {}, timeout: 1)

      expect(messages.size).to eq(1)
      messages.map(&:content).each do |number|
        expect(number).to be_between(0, 100)
      end

      em_subscribers.each(&:stop)
    end

    it "can request_all and receive a set of messages (mode broadcast)" do
      Verse.start(
        :test,
        config_path: "./spec/spec_data/config.yml"
      )

      em_subscribers = 5.times.map do |x|
        em = Verse::Redis::Stream::EventManager.new(
          service_name: "verse_spec",
          service_id: x, # random ID
          config: {},
          logger: Verse.logger
        )

        em.subscribe(
          "example:random_number",
          mode: Verse::Event::Manager::MODE_BROADCAST
        ) do |message, _channel|
          message.reply(rand(100))
        end

        em.start
        em
      end

      messages = Verse.request_all("example:random_number", {}, timeout: 1)
      expect(messages.size).to eq(5)

      messages.map(&:content).each do |number|
        expect(number).to be_between(0, 100)
      end

      em_subscribers.each(&:stop)
    end
  end
end
