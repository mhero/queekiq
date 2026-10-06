require "queekiq/version"
require "queekiq/configuration"
require "queekiq/message"
require "queekiq/queue"
require "queekiq/worker"

module Queekiq
  class Error < StandardError; end

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

    # Drops all configuration, including the memoized Redis client.
    def reset!
      @configuration = nil
    end
  end
end
