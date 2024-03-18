# frozen_string_literal: true

require "csv"

module Verse
  module Http
    module Exposition
      # A hook is a single endpoint on the http server
      # @see Verse::Http::Exposition::Extension#on_http
      # @see Verse::Exposition::Base#expose
      class Hook < Verse::Exposition::Hook::Base
        attr_reader :type

        # Create a new hook
        # Used internally by the `on_http` method.
        # @see Verse::Http::Exposition::Extension#on_http
        def initialize(exposition, cron)
          super(exposition)
          @cron = cron
        end

        # :nodoc:
        def register_impl
          hook = self

          binding.pry

          CronTask.new do
          end
        end
      end
    end
  end
end
