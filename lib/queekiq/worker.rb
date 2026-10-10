require "socket"

module Queekiq
  # The consumer loop. Each iteration it promotes due deferrals, picks up
  # messages other consumers abandoned, then blocks briefly for new ones; every
  # message goes through the handler and is then settled.
  #
  #   worker = Queekiq::Worker.new(queue: queue) do |message|
  #     process(message.payload)   # return an outcome, e.g. :done or :deferred
  #   end
  #   worker.start                 # traps INT/TERM and runs until stopped
  #
  # What happens to a message after the handler:
  #
  # * returns an outcome in `defer_on` (default :deferred): the message is
  #   scheduled to come back later (Queue#defer), or dead-lettered once it has
  #   been deferred max_defers times;
  # * returns anything else: it is acked;
  # * raises: the error is logged and passed to `on_error`, then the message is
  #   retried after a backoff (Queue#retry_later), or dead-lettered once it has
  #   used up max_attempts.
  #
  # While the handler runs, a heartbeat keeps the message from being reclaimed,
  # so slow jobs are fine. If the worker process dies, the heartbeat stops and
  # another worker reclaims the message after reclaim_after_ms.
  #
  # Lost connections are retried with backoff instead of crashing the worker,
  # and a stream or group that vanished (Redis restarted empty) is recreated.
  class Worker
    Failure = Struct.new(:error)
    private_constant :Failure

    DEFAULT_SHUTDOWN_TIMEOUT = 25

    attr_reader :queue, :consumer

    def self.default_consumer
      "#{Socket.gethostname}-#{Process.pid}"
    end

    # handler               - anything responding to #call(message); alternatively pass a block.
    # consumer              - unique per worker process (default: hostname + pid).
    # defer_on              - outcomes that trigger Queue#defer.
    # block_ms              - how long one read blocks.
    # on_error              - optional #call(error, message), e.g. to report to an error tracker.
    # heartbeat_interval_ms - how often to touch the message being handled; default is a third
    #                         of the queue's reclaim_after_ms, false disables it.
    # shutdown_timeout      - seconds #stop waits for the message in hand before abandoning it
    #                         (it is then reclaimed by another worker); nil waits forever.
    # reconnect_backoff     - pause between attempts while Redis is unreachable.
    def initialize(queue:, handler: nil, consumer: self.class.default_consumer, logger: nil,
                   defer_on: [ :deferred ], block_ms: 1_000, on_error: nil, heartbeat_interval_ms: nil,
                   shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
                   reconnect_backoff: Backoff.new(base_ms: 500, max_ms: 30_000), &block)
      @handler = handler || block
      raise ArgumentError, "pass a handler: or a block" unless @handler

      @queue = queue
      @consumer = consumer
      @logger = logger
      @defer_on = Array(defer_on)
      @block_ms = block_ms
      @on_error = on_error
      @heartbeat_interval_ms = heartbeat_interval_ms
      @shutdown_timeout = shutdown_timeout
      @reconnect_backoff = reconnect_backoff
      @running = true
    end

    def logger
      @logger || Queekiq.logger
    end

    # Ask the loop to finish the message in hand and return. If that takes longer
    # than shutdown_timeout, the message is abandoned: it stays pending and
    # another worker reclaims it. Safe to call from a signal handler.
    def stop
      @running = false
      start_watchdog
    end

    # Convenience for worker scripts: unbuffered stdout, graceful stop on
    # INT/TERM, then #run.
    def start(signals: %w[INT TERM])
      $stdout.sync = true
      signals.each { |signal| trap(signal) { stop } }
      run
    end

    def run
      @worker_thread = Thread.current
      log(:info, "started")
      supervise
      log(:info, "stopped")
    rescue ShutdownTimeout
      log(:warn, "shutdown_timeout", timeout: @shutdown_timeout)
    ensure
      @watchdog&.kill
    end

    # Processes messages in the order given. Public so it can be unit tested
    # without a running loop.
    def handle(messages)
      messages.each { |message| process(message) }
    end

    private

    # The loop, plus recovery from Redis going away.
    def supervise
      ready = false
      failures = 0
      while @running
        begin
          ready ||= prepare
          iterate
          failures = 0
        rescue Redis::BaseConnectionError => e
          ready = false
          failures += 1
          wait_for_redis(e, failures)
        rescue Redis::CommandError => e
          # NOGROUP: the stream or group is gone. UNBLOCKED: it was deleted while
          # we were blocked reading it. Either way, recreate it and carry on.
          raise unless e.message.start_with?("NOGROUP", "UNBLOCKED")

          ready = false
        end
      end
    end

    def prepare
      queue.ensure_group!
      true
    end

    def iterate
      queue.promote_due!
      reclaimed = queue.reclaim(consumer: consumer)
      instrument("reclaim.queekiq", count: reclaimed.size) unless reclaimed.empty?
      handle(reclaimed)
      handle(queue.read(consumer: consumer, block_ms: @block_ms))
    end

    def wait_for_redis(error, failures)
      delay = @reconnect_backoff.delay_ms(failures)
      instrument("connection_error.queekiq", error: error, retry_in_ms: delay, failures: failures)
      log(:warn, "redis_unavailable", error: "#{error.class}: #{error.message}", retry_in_ms: delay)
      pause(delay)
    end

    def process(message)
      return dead_letter(message, :max_attempts) if queue.attempts_exhausted?(message)

      outcome = run_handler(message)
      return fail_message(message, outcome.error) if outcome.is_a?(Failure)

      settle(message, outcome)
    end

    def run_handler(message)
      instrument("process.queekiq", event(message, latency_ms: message.latency_ms)) do |payload|
        outcome = with_heartbeat(message) { @handler.call(message) }
        payload[:outcome] = outcome
      end
    rescue StandardError => e
      Failure.new(e)
    end

    def settle(message, outcome)
      if @defer_on.include?(outcome)
        return dead_letter(message, :max_defers) if queue.defers_exhausted?(message)

        delay = queue.defer(message)
        instrument("defer.queekiq", event(message, delay_ms: delay))
        log(:info, "deferred", message, outcome: outcome, delay_ms: delay)
      else
        queue.ack(message)
        log(:info, "processed", message, outcome: outcome)
      end
    end

    def fail_message(message, error)
      log(:error, "failed", message, error: "#{error.class}: #{error.message}")
      report(error, message)
      return dead_letter(message, :max_attempts, error: error) if queue.last_attempt?(message)

      delay = queue.retry_later(message)
      instrument("retry.queekiq", event(message, delay_ms: delay, error: error))
      log(:warn, "retry", message, delay_ms: delay)
    end

    def dead_letter(message, reason, error: nil)
      queue.dead_letter(message, reason: reason, error: error)
      instrument("dead_letter.queekiq", event(message, reason: reason, error: error))
      log(:error, "dead_letter", message, reason: reason)
    end

    def report(error, message)
      @on_error&.call(error, message)
    rescue StandardError => e
      log(:error, "on_error_failed", message, error: "#{e.class}: #{e.message}")
    end

    # --- heartbeat -----------------------------------------------------------

    def heartbeat_ms
      return nil if @heartbeat_interval_ms == false

      @heartbeat_interval_ms || (queue.reclaim_after_ms / 3)
    end

    # Runs the block while a background thread keeps touching the message. The
    # thread is stopped by a flag (never killed mid-command), so it can't leave
    # the shared Redis connection in a half-read state.
    def with_heartbeat(message)
      interval = heartbeat_ms
      return yield unless interval

      lock = Mutex.new
      wake = ConditionVariable.new
      finished = false
      thread = Thread.new do
        Thread.current.report_on_exception = false
        lock.synchronize do
          until finished
            wake.wait(lock, interval / 1000.0)
            break if finished || !beat(message)
          end
        end
      end
      begin
        yield
      ensure
        lock.synchronize do
          finished = true
          wake.signal
        end
        thread.join
      end
    end

    # Returns false when the message is no longer ours (stop beating), true
    # otherwise, including on errors: a Redis blip shouldn't end the heartbeat.
    def beat(message)
      return true if queue.touch(message, consumer: consumer)

      log(:warn, "heartbeat_lost", message)
      false
    rescue StandardError => e
      log(:warn, "heartbeat_failed", message, error: "#{e.class}: #{e.message}")
      true
    end

    # --- shutdown ------------------------------------------------------------

    def start_watchdog
      worker = @worker_thread
      return if @watchdog || !@shutdown_timeout || !worker&.alive?

      @watchdog = Thread.new do
        sleep @shutdown_timeout
        worker.raise(ShutdownTimeout, "worker did not stop within #{@shutdown_timeout}s") if worker.alive?
      end
    end

    # Sleeps up to ms milliseconds, waking early if the worker is stopped.
    def pause(ms)
      deadline = monotonic + (ms / 1000.0)
      sleep([ 0.1, deadline - monotonic ].min) while @running && monotonic < deadline
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # --- events and logs -----------------------------------------------------

    def instrument(name, payload = {}, &block)
      Queekiq.instrumenter.instrument(name, { queue: queue.name, consumer: consumer }.merge(payload), &block)
    end

    def event(message, **extra)
      { message: message }.merge(extra)
    end

    # One logfmt-style line: [queekiq] event=processed queue=... id=... outcome=done
    def log(level, event, message = nil, **fields)
      parts = [ "[queekiq]", "event=#{event}", "queue=#{queue.name}", "consumer=#{consumer}" ]
      parts << "id=#{message.id}" << "payload=#{format_value(message.raw[0, 100])}" << "attempt=#{message.attempt}" if message
      fields.each { |key, value| parts << "#{key}=#{format_value(value)}" }
      logger.public_send(level, parts.join(" "))
    end

    def format_value(value)
      text = value.nil? ? "nil" : value.to_s
      text.match?(%r{\A[\w.:@/-]+\z}) ? text : text.inspect
    end
  end
end
