require "logger"
require "redis"

module Queekiq
  # Global defaults shared by every Queue and Worker that doesn't get its own
  # `redis:` / `logger:` argument.
  class Configuration
    DEFAULT_REDIS_URL = "redis://localhost:6379/0".freeze

    attr_accessor :redis_url
    attr_writer :redis, :logger

    def initialize
      @redis_url = ENV.fetch("REDIS_URL", DEFAULT_REDIS_URL)
    end

    # A shared client, built lazily from `redis_url` unless one was assigned.
    def redis
      @redis ||= Redis.new(url: redis_url)
    end

    # Falls back to Rails.logger when running inside Rails, otherwise to a
    # logger on $stdout.
    def logger
      @logger || default_logger
    end

    private

    def default_logger
      if defined?(::Rails) && ::Rails.respond_to?(:logger) && ::Rails.logger
        ::Rails.logger
      else
        @default_logger ||= Logger.new($stdout)
      end
    end
  end
end
