module Queekiq
  # A tiny in-process event bus, so metrics and tracing can hook in without
  # Queekiq depending on any monitoring library.
  #
  #   Queekiq.subscribe("process.queekiq") do |name, payload|
  #     StatsD.timing("queekiq.process", payload[:duration_ms])
  #   end
  #
  # Workers call `instrument(name, payload) { |payload| ... }`. The block's
  # duration is added to the payload as :duration_ms and, if it raises, the error
  # as :exception. Subscribers that raise are ignored.
  #
  # The call shape matches ActiveSupport::Notifications, so in Rails you can use
  # it instead: `Queekiq.configure { |c| c.instrumenter = ActiveSupport::Notifications }`
  # (then subscribe through ActiveSupport).
  class Instrumenter
    def initialize
      @subscribers = []
    end

    # pattern: nil (every event), a String (exact event name) or a Regexp.
    def subscribe(pattern = nil, &block)
      raise ArgumentError, "a block is required" unless block

      @subscribers << [ pattern, block ]
      block
    end

    def instrument(name, payload = {})
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      begin
        block_given? ? yield(payload) : nil
      rescue StandardError => e
        payload[:exception] = [ e.class.name, e.message ]
        raise
      ensure
        payload[:duration_ms] = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1000).round(2)
        publish(name, payload)
      end
    end

    private

    def publish(name, payload)
      @subscribers.each do |pattern, block|
        next unless pattern.nil? || pattern === name # rubocop:disable Style/CaseEquality

        block.call(name, payload)
      rescue StandardError
        nil # a broken subscriber must never break message processing
      end
    end
  end
end
