require "spec_helper"

RSpec.describe Queekiq::Worker do
  def message(id, payload)
    Queekiq::Message.new(id: id, payload: payload)
  end

  let(:queue) { instance_double(Queekiq::Queue, name: "test", ack: true, defer: true) }
  let(:handler) { double("handler", call: :completed) }
  let(:worker) { described_class.new(queue: queue, handler: handler, consumer: "test", logger: Logger.new(nil)) }

  describe "#initialize" do
    it "requires a handler or a block" do
      expect { described_class.new(queue: queue) }.to raise_error(ArgumentError, /handler/)
    end

    it "accepts a block as the handler" do
      seen = []
      block_worker = described_class.new(queue: queue, logger: Logger.new(nil)) { |m| seen << m.payload }

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

      expect(handler).to have_received(:call).with(first)
      expect(handler).to have_received(:call).with(second)
      expect(queue).to have_received(:ack).with(first)
      expect(queue).to have_received(:ack).with(second)
    end

    it "defers a :deferred outcome in addition to acking the delivery" do
      allow(handler).to receive(:call).and_return(:deferred)
      msg = message("1-0", "10")

      worker.handle([ msg ])

      expect(queue).to have_received(:defer).with("10")
      expect(queue).to have_received(:ack).with(msg)
    end

    it "does not defer for any other outcome" do
      [ :completed, :already_processed, :not_found, :failed, nil ].each do |outcome|
        allow(handler).to receive(:call).and_return(outcome)
        worker.handle([ message("1-0", "10") ])
      end

      expect(queue).not_to have_received(:defer)
    end

    it "defers on custom outcomes via defer_on" do
      custom = described_class.new(queue: queue, handler: handler, defer_on: %i[busy later], logger: Logger.new(nil))
      allow(handler).to receive(:call).and_return(:later)

      custom.handle([ message("1-0", "10") ])

      expect(queue).to have_received(:defer).with("10")
    end

    it "still acks when the outcome is a failure outcome" do
      allow(handler).to receive(:call).and_return(:failed)
      msg = message("1-0", "10")

      worker.handle([ msg ])

      expect(queue).to have_received(:ack).with(msg)
    end

    it "does nothing for an empty list" do
      worker.handle([])

      expect(handler).not_to have_received(:call)
      expect(queue).not_to have_received(:ack)
    end

    it "logs the outcome for each processed message" do
      logger = instance_double(Logger, info: nil)
      logging_worker = described_class.new(queue: queue, handler: handler, consumer: "test", logger: logger)

      logging_worker.handle([ message("1-0", "10") ])

      expect(logger).to have_received(:info).with(a_string_matching(/payload=10 outcome=:completed/))
    end

    it "processes messages in the order given" do
      first = message("1-0", "10")
      second = message("2-0", "11")

      worker.handle([ first, second ])

      expect(handler).to have_received(:call).with(first).ordered
      expect(handler).to have_received(:call).with(second).ordered
    end

    context "when the handler raises" do
      let(:boom) { RuntimeError.new("boom") }

      before { allow(handler).to receive(:call).and_raise(boom) }

      it "does not ack or defer, leaving the message pending for reclaim" do
        worker.handle([ message("1-0", "10") ])

        expect(queue).not_to have_received(:ack)
        expect(queue).not_to have_received(:defer)
      end

      it "keeps going with the remaining messages" do
        allow(handler).to receive(:call).with(satisfy { |m| m.payload == "11" }).and_return(:completed)
        good = message("2-0", "11")

        worker.handle([ message("1-0", "10"), good ])

        expect(queue).to have_received(:ack).with(good)
      end

      it "logs the error and calls on_error with the error and message" do
        logger = instance_double(Logger, info: nil, error: nil)
        reported = []
        reporting = described_class.new(queue: queue, handler: handler, logger: logger,
                                        on_error: ->(error, msg) { reported << [ error, msg ] })
        msg = message("1-0", "10")

        reporting.handle([ msg ])

        expect(logger).to have_received(:error).with(a_string_matching(/payload=10 failed: RuntimeError: boom/))
        expect(reported).to eq([ [ boom, msg ] ])
      end
    end
  end

  describe "#run" do
    let(:queue) do
      instance_double(Queekiq::Queue, name: "test", ensure_group!: true, promote_due!: 0,
                                      reclaim: [], ack: true, defer: true)
    end

    it "ensures the consumer group exists once before looping" do
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

    it "honours block_ms" do
      fast = described_class.new(queue: queue, handler: handler, consumer: "test", block_ms: 50, logger: Logger.new(nil))
      allow(queue).to receive(:read) { fast.stop; [] }

      fast.run

      expect(queue).to have_received(:read).with(consumer: "test", block_ms: 50)
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

    it "does not iterate again after #stop, even if reclaim would still find work" do
      allow(queue).to receive(:reclaim).and_return([ message("r-1", "still-pending") ])
      allow(queue).to receive(:read) { worker.stop; [] }

      worker.run

      expect(queue).to have_received(:read).once
      expect(queue).to have_received(:reclaim).once
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
end
