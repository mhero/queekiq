require "socket"

module Queekiq
  # The consumer loop. Each iteration it promotes due deferrals, picks up
  # messages other consumers abandoned, then blocks briefly for new ones; every
  # message goes through the handler and is acked afterwards.
  #
  #   worker = Queekiq::Worker.new(queue: queue) do |message|
  #     process(message.payload)   # return an outcome, e.g. :done or :deferred
  #   end
  #   worker.start                 # traps INT/TERM and runs until stopped
  #
  # The handler receives a Queekiq::Message and may return anything. If the
  # outcome is one of `defer_on` (default: :deferred) the payload is put on the
  # queue's deferred set to be retried later. After the handler returns, the
  # message is acked whatever the outcome.
  #
  # If the handler raises, the error is logged, `on_error` is called, and the
  # message is left unacked: it will be redelivered once the queue's
  # reclaim_after_ms has passed. A handler that must not be retried forever
  # should rescue its own errors and return an outcome instead.
  class Worker
    attr_reader :queue, :consumer

    def self.default_consumer
      "#{Socket.gethostname}-#{Process.pid}"
    end

    # handler  - anything responding to #call(message); alternatively pass a block.
    # consumer - unique per worker process (default: hostname + pid).
    # defer_on - outcomes that trigger Queue#defer.
    # block_ms - how long one read blocks; also the worst-case stop latency.
    # on_error - optional #call(error, message), e.g. to report to an error tracker.
    def initialize(queue:, handler: nil, consumer: self.class.default_consumer, logger: nil,
                   defer_on: [ :deferred ], block_ms: 1_000, on_error: nil, &block)
      @handler = handler || block
      raise ArgumentError, "pass a handler: or a block" unless @handler

      @queue = queue
      @consumer = consumer
      @logger = logger
      @defer_on = Array(defer_on)
      @block_ms = block_ms
      @on_error = on_error
      @running = true
    end

    def logger
      @logger || Queekiq.logger
    end

    # Ask the loop to finish its current iteration and return.
    def stop
      @running = false
    end

    # Convenience for worker scripts: unbuffered stdout, graceful stop on
    # INT/TERM, then #run.
    def start(signals: %w[INT TERM])
      $stdout.sync = true
      signals.each { |signal| trap(signal) { stop } }
      run
    end

    def run
      queue.ensure_group!
      logger.info("[queekiq] #{consumer} started on #{queue.name}")

      while @running
        queue.promote_due!
        handle(queue.reclaim(consumer: consumer))
        handle(queue.read(consumer: consumer, block_ms: @block_ms))
      end

      logger.info("[queekiq] #{consumer} stopped")
    end

    # Processes messages in the order given. Public so it can be unit tested
    # without a running loop.
    def handle(messages)
      messages.each { |message| process(message) }
    end

    private

    def process(message)
      outcome = @handler.call(message)
      queue.defer(message.payload) if @defer_on.include?(outcome)
      logger.info("[queekiq] #{consumer} payload=#{message.payload} outcome=#{outcome.inspect}")
      queue.ack(message)
    rescue StandardError => e
      logger.error("[queekiq] #{consumer} payload=#{message.payload} failed: #{e.class}: #{e.message}")
      @on_error&.call(e, message)
    end
  end
end
