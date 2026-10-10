require "spec_helper"

RSpec.describe Queekiq::Worker do
  def message(id, payload, **options)
    Queekiq::Message.new(id: id, payload: payload, **options)
  end

  let(:queue) do
    instance_double(Queekiq::Queue,
                    name: "test", ack: true, defer: 200, retry_later: 1_000, dead_letter: true,
                    attempts_exhausted?: false, last_attempt?: false, defers_exhausted?: false,
                    touch: true, reclaim_after_ms: 30_000)
  end
  let(:handler) { double("handler", call: :completed) }
  let(:logger) { Logger.new(nil) }
  let(:options) { {} }
  let(:worker) do
    described_class.new(queue: queue, handler: handler, consumer: "test", logger: logger,
                        heartbeat_interval_ms: false, **options)
  end

  describe "#initialize" do
    it "requires a handler or a block" do
      expect { described_class.new(queue: queue) }.to raise_error(ArgumentError, /handler/)
    end

    it "accepts a block as the handler" do
      seen = []
      block_worker = described_class.new(queue: queue, logger: logger, heartbeat_interval_ms: false) do |m|
        seen << m.payload
      end

      block_worker.handle([ message("1-0", "10") ])

      expect(seen).to eq([ "10" ])
    end

    it "defaults the consumer to hostname + pid and the logger to Queekiq.logger" do
      default_worker = described_class.new(queue: queue, handler: handler)

      expect(default_worker.consumer).to eq("#{Socket.gethostname}-#{Process.pid}")
      expect(default_worker.logger).to equal(Queekiq.logger)
    end
  end

  describe "#handle" do
    it "passes each message to the handler and acks it" do
      first = message("1-0", "10")
      second = message("2-0", "11")

      worker.handle([ first, second ])

      expect(handler).to have_received(:call).with(first).ordered
      expect(handler).to have_received(:call).with(second).ordered
      expect(queue).to have_received(:ack).with(first)
      expect(queue).to have_received(:ack).with(second)
    end

    it "does nothing for an empty list" do
      worker.handle([])

      expect(handler).not_to have_received(:call)
      expect(queue).not_to have_received(:ack)
    end

    it "logs one structured line per processed message" do
      lines = []
      allow(logger).to receive(:info) { |line| lines << line }

      worker.handle([ message("1-0", "10") ])

      expect(lines.last).to eq("[queekiq] event=processed queue=test consumer=test id=1-0 payload=10 attempt=1 outcome=completed")
    end

    it "quotes log values that contain spaces and truncates long payloads" do
      lines = []
      allow(logger).to receive(:info) { |line| lines << line }

      worker.handle([ message("1-0", "a b#{"x" * 500}") ])

      expect(lines.last).to match(/payload="a bx+"/)
      expect(lines.last.size).to be < 300
    end

    it "passes the message to the handler with its envelope" do
      seen = nil
      allow(handler).to receive(:call) { |m| seen = m }

      worker.handle([ message("1-0", "10", attempt: 3, defers: 2, enqueued_at: 5) ])

      expect([ seen.attempt, seen.defers, seen.enqueued_at ]).to eq([ 3, 2, 5 ])
    end

    describe "deferral" do
      before { allow(handler).to receive(:call).and_return(:deferred) }

      it "defers the message (which also acks the delivery) instead of acking it" do
        msg = message("1-0", "10")

        worker.handle([ msg ])

        expect(queue).to have_received(:defer).with(msg)
        expect(queue).not_to have_received(:ack)
      end

      it "does not defer for any other outcome, and acks" do
        [ :completed, :already_processed, :not_found, :failed, nil ].each do |outcome|
          allow(handler).to receive(:call).and_return(outcome)
          worker.handle([ message("1-0", "10") ])
        end

        expect(queue).not_to have_received(:defer)
        expect(queue).to have_received(:ack).exactly(5).times
      end

      context "with custom defer_on" do
        let(:options) { { defer_on: %i[busy later] } }

        it "defers on those outcomes" do
          allow(handler).to receive(:call).and_return(:later)

          worker.handle([ message("1-0", "10") ])

          expect(queue).to have_received(:defer)
        end

        it "no longer defers on :deferred" do
          worker.handle([ message("1-0", "10") ])

          expect(queue).not_to have_received(:defer)
        end
      end

      it "dead-letters instead of deferring once the defer limit is reached" do
        allow(queue).to receive(:defers_exhausted?).and_return(true)
        msg = message("1-0", "10")

        worker.handle([ msg ])

        expect(queue).to have_received(:dead_letter).with(msg, reason: :max_defers, error: nil)
        expect(queue).not_to have_received(:defer)
      end
    end

    describe "failures" do
      let(:boom) { RuntimeError.new("boom") }

      before { allow(handler).to receive(:call).and_raise(boom) }

      it "retries the message later instead of acking it" do
        msg = message("1-0", "10")

        worker.handle([ msg ])

        expect(queue).to have_received(:retry_later).with(msg)
        expect(queue).not_to have_received(:ack)
        expect(queue).not_to have_received(:dead_letter)
      end

      it "dead-letters on the last attempt, with the error" do
        allow(queue).to receive(:last_attempt?).and_return(true)
        msg = message("1-0", "10")

        worker.handle([ msg ])

        expect(queue).to have_received(:dead_letter).with(msg, reason: :max_attempts, error: boom)
        expect(queue).not_to have_received(:retry_later)
      end

      it "keeps going with the remaining messages" do
        good = message("2-0", "11")
        allow(handler).to receive(:call).with(good).and_return(:completed)

        worker.handle([ message("1-0", "10"), good ])

        expect(queue).to have_received(:ack).with(good)
      end

      it "logs the error and calls on_error with the error and message" do
        errors = []
        lines = []
        allow(logger).to receive(:error) { |line| lines << line }
        reporting = described_class.new(queue: queue, handler: handler, logger: logger, heartbeat_interval_ms: false,
                                        on_error: ->(error, msg) { errors << [ error, msg ] })
        msg = message("1-0", "10")

        reporting.handle([ msg ])

        expect(lines.first).to include("event=failed", "payload=10", 'error="RuntimeError: boom"')
        expect(errors).to eq([ [ boom, msg ] ])
      end

      it "survives an on_error callback that raises" do
        reporting = described_class.new(queue: queue, handler: handler, logger: logger, heartbeat_interval_ms: false,
                                        on_error: ->(_error, _msg) { raise "reporter down" })

        expect { reporting.handle([ message("1-0", "10") ]) }.not_to raise_error
        expect(queue).to have_received(:retry_later)
      end

      it "treats a payload that can't be decoded like any other handler failure" do
        bad = Queekiq::Message.new(id: "1-0", raw: "{nope", serializer: Queekiq::Serializers::JSON)
        decoding = described_class.new(queue: queue, handler: lambda(&:payload), logger: logger,
                                       heartbeat_interval_ms: false)

        decoding.handle([ bad ])

        expect(queue).to have_received(:retry_later).with(bad)
      end
    end

    it "dead-letters a message that already used up its attempts without running the handler" do
      allow(queue).to receive(:attempts_exhausted?).and_return(true)
      msg = message("1-0", "10", attempt: 6)

      worker.handle([ msg ])

      expect(handler).not_to have_received(:call)
      expect(queue).to have_received(:dead_letter).with(msg, reason: :max_attempts, error: nil)
    end

    it "lets Redis errors from settling a message escape, leaving it pending" do
      allow(queue).to receive(:ack).and_raise(Redis::CannotConnectError)

      expect { worker.handle([ message("1-0", "10") ]) }.to raise_error(Redis::CannotConnectError)
      expect(queue).not_to have_received(:retry_later)
    end

    describe "instrumentation" do
      let(:events) { [] }

      before { Queekiq.subscribe { |name, payload| events << [ name, payload ] } }

      def event(name)
        events.find { |n, _| n == name }&.last
      end

      it "emits process.queekiq with the outcome, duration and latency" do
        msg = message("1-0", "10", enqueued_at: (Time.now.to_f * 1000).to_i - 50)

        worker.handle([ msg ])

        payload = event("process.queekiq")
        expect(payload).to include(queue: "test", consumer: "test", message: msg, outcome: :completed)
        expect(payload[:duration_ms]).to be_a(Numeric)
        expect(payload[:latency_ms]).to be >= 50
      end

      it "records the exception on process.queekiq when the handler raises" do
        allow(handler).to receive(:call).and_raise(RuntimeError, "boom")

        worker.handle([ message("1-0", "10") ])

        expect(event("process.queekiq")[:exception]).to eq(%w[RuntimeError boom])
      end

      it "emits defer.queekiq with the delay" do
        allow(handler).to receive(:call).and_return(:deferred)

        worker.handle([ message("1-0", "10") ])

        expect(event("defer.queekiq")).to include(delay_ms: 200)
      end

      it "emits retry.queekiq with the delay and error" do
        error = RuntimeError.new("boom")
        allow(handler).to receive(:call).and_raise(error)

        worker.handle([ message("1-0", "10") ])

        expect(event("retry.queekiq")).to include(delay_ms: 1_000, error: error)
      end

      it "emits dead_letter.queekiq with the reason" do
        allow(queue).to receive(:last_attempt?).and_return(true)
        allow(handler).to receive(:call).and_raise(RuntimeError, "boom")

        worker.handle([ message("1-0", "10") ])

        expect(event("dead_letter.queekiq")).to include(reason: :max_attempts)
      end
    end
  end

  describe "#run" do
    let(:queue) do
      instance_double(Queekiq::Queue, name: "test", ensure_group!: true, promote_due!: 0, reclaim: [],
                                      ack: true, defer: 200, retry_later: 1_000, dead_letter: true,
                                      attempts_exhausted?: false, last_attempt?: false, defers_exhausted?: false,
                                      touch: true, reclaim_after_ms: 30_000)
    end

    it "ensures the consumer group exists before looping" do
      allow(queue).to receive(:read) { worker.stop; [] }

      worker.run

      expect(queue).to have_received(:ensure_group!).once
    end

    it "promotes due deferrals and reclaims stalled messages for the configured consumer" do
      allow(queue).to receive(:read) { worker.stop; [] }

      worker.run

      expect(queue).to have_received(:promote_due!).once
      expect(queue).to have_received(:reclaim).with(consumer: "test").once
    end

    it "reads with the configured consumer and a 1 second block by default" do
      allow(queue).to receive(:read) { worker.stop; [] }

      worker.run

      expect(queue).to have_received(:read).with(consumer: "test", block_ms: 1_000)
    end

    context "with block_ms" do
      let(:options) { { block_ms: 50 } }

      it "reads with that block time" do
        allow(queue).to receive(:read) { worker.stop; [] }

        worker.run

        expect(queue).to have_received(:read).with(consumer: "test", block_ms: 50)
      end
    end

    it "stops looping once #stop is called, not before" do
      calls = 0
      allow(queue).to receive(:read) do
        calls += 1
        worker.stop if calls >= 3
        []
      end

      worker.run

      expect(calls).to eq(3)
      expect(queue).to have_received(:reclaim).exactly(3).times
    end

    it "processes messages from both reclaim and read in the same iteration" do
      reclaimed = message("r-1", "reclaimed")
      fresh = message("m-1", "fresh")
      allow(queue).to receive(:reclaim).and_return([ reclaimed ])
      allow(queue).to receive(:read) { worker.stop; [ fresh ] }

      worker.run

      expect(handler).to have_received(:call).with(reclaimed)
      expect(handler).to have_received(:call).with(fresh)
    end

    it "emits reclaim.queekiq when it took over abandoned messages" do
      events = []
      Queekiq.subscribe("reclaim.queekiq") { |_, payload| events << payload[:count] }
      allow(queue).to receive(:reclaim).and_return([ message("r-1", "a"), message("r-2", "b") ])
      allow(queue).to receive(:read) { worker.stop; [] }

      worker.run

      expect(events).to eq([ 2 ])
    end

    describe "when Redis is unavailable" do
      let(:options) { { reconnect_backoff: Queekiq::Backoff.fixed(5) } }

      it "backs off and tries again instead of crashing" do
        attempts = 0
        allow(queue).to receive(:promote_due!) do
          attempts += 1
          raise Redis::CannotConnectError, "down" if attempts <= 2

          0
        end
        allow(queue).to receive(:read) { worker.stop; [] }

        expect { worker.run }.not_to raise_error

        expect(attempts).to eq(3)
      end

      it "re-ensures the consumer group after reconnecting, in case Redis restarted empty" do
        attempts = 0
        allow(queue).to receive(:promote_due!) do
          attempts += 1
          raise Redis::CannotConnectError, "down" if attempts == 1

          0
        end
        allow(queue).to receive(:read) { worker.stop; [] }

        worker.run

        expect(queue).to have_received(:ensure_group!).twice
      end

      it "survives Redis being down at startup" do
        calls = 0
        allow(queue).to receive(:ensure_group!) do
          calls += 1
          raise Redis::CannotConnectError, "down" if calls == 1

          true
        end
        allow(queue).to receive(:read) { worker.stop; [] }

        expect { worker.run }.not_to raise_error
        expect(queue).to have_received(:ensure_group!).twice
      end

      it "emits connection_error.queekiq with the failure count" do
        events = []
        Queekiq.subscribe("connection_error.queekiq") { |_, payload| events << payload.values_at(:failures, :retry_in_ms) }
        attempts = 0
        allow(queue).to receive(:promote_due!) do
          attempts += 1
          raise Redis::CannotConnectError, "down" if attempts <= 2

          0
        end
        allow(queue).to receive(:read) { worker.stop; [] }

        worker.run

        expect(events).to eq([ [ 1, 5 ], [ 2, 5 ] ])
      end

      it "stops promptly when told to stop while waiting" do
        slow = described_class.new(queue: queue, handler: handler, consumer: "test", logger: logger,
                                   heartbeat_interval_ms: false, reconnect_backoff: Queekiq::Backoff.fixed(60_000))
        allow(queue).to receive(:promote_due!).and_raise(Redis::CannotConnectError, "down")
        thread = Thread.new { slow.run }
        sleep 0.1

        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        slow.stop
        thread.join(2)

        expect(thread).not_to be_alive
        expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
      end
    end

    it "recreates the consumer group when Redis reports it missing" do
      calls = 0
      allow(queue).to receive(:reclaim) do
        calls += 1
        raise Redis::CommandError, "NOGROUP No such key 'x' or consumer group 'g'" if calls == 1

        []
      end
      allow(queue).to receive(:read) { worker.stop; [] }

      worker.run

      expect(queue).to have_received(:ensure_group!).twice
    end

    it "recovers when the stream is deleted while a read is blocked" do
      calls = 0
      allow(queue).to receive(:read) do
        calls += 1
        raise Redis::CommandError, "UNBLOCKED the stream key no longer exists" if calls == 1

        worker.stop
        []
      end

      worker.run

      expect(queue).to have_received(:ensure_group!).twice
    end

    it "does not hide other Redis command errors" do
      allow(queue).to receive(:reclaim).and_raise(Redis::CommandError, "WRONGTYPE Operation against a key")

      expect { worker.run }.to raise_error(Redis::CommandError, /WRONGTYPE/)
    end
  end

  describe "#start" do
    it "traps the given signals to stop the worker, then runs" do
      allow(worker).to receive(:trap)
      allow(worker).to receive(:run)

      worker.start(signals: %w[USR1])

      expect(worker).to have_received(:trap).with("USR1")
      expect(worker).to have_received(:run)
    end
  end

  describe "heartbeat interval" do
    it "defaults to a third of the queue's reclaim window" do
      default = described_class.new(queue: queue, handler: handler, logger: logger)

      expect(default.send(:heartbeat_ms)).to eq(10_000)
    end

    it "can be set, or switched off with false" do
      custom = described_class.new(queue: queue, handler: handler, logger: logger, heartbeat_interval_ms: 250)

      expect(custom.send(:heartbeat_ms)).to eq(250)
      expect(worker.send(:heartbeat_ms)).to be_nil
    end
  end
end
