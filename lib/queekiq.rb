require "queekiq/version"

module Queekiq
  class Error < StandardError; end

  # A payload couldn't be encoded or decoded by the queue's serializer.
  class SerializationError < Error; end

  # Raised inside a worker whose shutdown_timeout ran out while a message was
  # still being handled. Inherits from Exception so a handler's own
  # `rescue StandardError` can't swallow it.
  class ShutdownTimeout < Exception; end # rubocop:disable Lint/InheritException
end

require "queekiq/serializers"
require "queekiq/backoff"
require "queekiq/instrumenter"
require "queekiq/configuration"
require "queekiq/message"
require "queekiq/queue"
require "queekiq/worker"

module Queekiq
  class << self
    def configuration
      @configuration ||= Configuration.new
    end

    # Queekiq.configure { |c| c.redis_url = "redis://redis:6379/1" }
    def configure
      yield configuration
    end

    def redis
      configuration.redis
    end

    def logger
      configuration.logger
    end

    def instrumenter
      configuration.instrumenter
    end

    # Shorthand for Queekiq.instrumenter.subscribe(...).
    def subscribe(pattern = nil, &block)
      instrumenter.subscribe(pattern, &block)
    end

    # Drops all configuration, including the memoized Redis client.
    def reset!
      @configuration = nil
    end
  end
end
