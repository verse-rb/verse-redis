# frozen_string_literal: true

require "monitor"
require "verse/core"

require_relative "./config"

module Verse
  module Periodic
    # Add periodic hooks to the Verse expositions
    class Plugin < Verse::Plugin::Base
      attr_reader :config

      def description
        "Periodic hooks for Verse. Use of redis for locking mechanism."
      end

      def dependencies
        %i<redis>
      end

      def on_init
        @manager = Manager.new
      end

      def on_stop
        @manager.stop
      end

    end
  end
end
