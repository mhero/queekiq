require "spec_helper"

# End to end against a real Redis: Workers running their loop on a Queue.
RSpec.describe "Worker + Queue" do
  let(:name) { "spec-#{SecureRandom.hex(4)}" }
  let(:queue_options) { {} }
  let(:fast) { Queekiq::Backoff.fixed(20) }
  let(:lock) { Mutex.new }

  # One Redis connection per worker: a blocking read occupies its connection.
  def new_queue(**overrides)
    options = { reclaim_after_ms: 50, retry_backoff: fast, defer_backoff: fast }.merge(queue_options).merge(overrides)
    Queekiq::Queue.new(name, redis: Redis.new(url: Queekiq.configuration.redis_url), **options)
  end

  def build_worker(queue, consumer = "w1", **options, &handler)
    Queekiq::Worker.new(queue: queue, consumer: consumer, block_ms: 50, logger: Logger.new(nil),
                        handler: handler, **options)
  end

  def run_workers(*workers)
    threads = workers.map { |w| Thread.new { w.run } }
    yield
  ensure
    workers.each(&:stop)
    threads.each { |t| t.join(5) }
  end

  def wait_until(timeout: 5)
    deadline = Time.now + timeout
    sleep 0.01 until yield || Time.now > deadline
  end

  def synchronized(&block)
    lock.synchronize(&block)
  end

  let(:producer) { new_queue.tap(&:ensure_group!) }

  after { new_queue.clear! }

  it "processes every message exactly once across competing workers" do
    seen = []
    handler = ->(m) { synchronized { seen << m.payload }; :done }
    workers = %w[w1 w2].map { |c| build_worker(new_queue, c, &handler) }
    payloads = Array.new(20) { |i| "job-#{i}" }

    run_workers(*workers) do
      payloads.each { |p| producer.enqueue(p) }
      wait_until { synchronized { seen.size >= payloads.size } }
    end

    expect(seen.sort).to eq(payloads.sort)
  end

  it "carries structured payloads end to end with the JSON serializer" do
    received = []
    worker = build_worker(new_queue(serializer: Queekiq::Serializers::JSON)) do |m|
      synchronized { received << m.payload }
      :done
    end

    run_workers(worker) do
      new_queue(serializer: Queekiq::Serializers::JSON).tap(&:ensure_group!)
                                                       .enqueue("order" => 7, "lines" => [ { "sku" => "A" } ])
      wait_until { synchronized { received.any? } }
    end

    expect(received).to eq([ { "order" => 7, "lines" => [ { "sku" => "A" } ] } ])
  end

  describe "deferral" do
    it "retries a deferred message until its dependency is satisfied" do
      done = []
      worker = build_worker(new_queue) do |m|
        synchronized do
          next :deferred if m.payload == "b" && !done.include?("a")

          done << m.payload
          :done
        end
      end

      run_workers(worker) do
        producer.enqueue("b") # arrives first, but must wait for "a"
        producer.enqueue("a")
        wait_until { synchronized { done.size >= 2 } }
      end

      expect(done).to eq(%w[a b])
    end

    it "tells the handler how often a message has been deferred" do
      defers = []
      worker = build_worker(new_queue) do |m|
        synchronized { defers << m.defers }
        m.defers < 3 ? :deferred : :done
      end

      run_workers(worker) do
        producer.enqueue("job")
        wait_until { synchronized { defers.size >= 4 } }
      end

      expect(defers).to eq([ 0, 1, 2, 3 ])
    end

    context "with max_defers" do
      let(:queue_options) { { max_defers: 2 } }

      it "dead-letters a message that keeps being deferred" do
        calls = 0
        worker = build_worker(new_queue) { synchronized { calls += 1 }; :deferred }

        run_workers(worker) do
          producer.enqueue("never-ready")
          wait_until { producer.dead_size == 1 }
        end

        expect(calls).to eq(3)
        expect(producer.dead_messages.first.reason).to eq("max_defers")
        expect(producer.deferred_size).to eq(0)
      end
    end
  end

  describe "failures" do
    it "retries a failing message with backoff and passes the attempt number to the handler" do
      attempts = []
      worker = build_worker(new_queue) do |m|
        synchronized { attempts << m.attempt }
        raise "flaky" if m.attempt < 3

        :done
      end

      run_workers(worker) do
        producer.enqueue("job")
        wait_until { synchronized { attempts.size >= 3 } }
        wait_until { producer.stats[:pending].zero? && producer.deferred_size.zero? }
      end

      expect(attempts).to eq([ 1, 2, 3 ])
      expect(producer.dead_size).to eq(0)
    end

    context "with max_attempts" do
      let(:queue_options) { { max_attempts: 3 } }

      it "dead-letters a message that keeps failing, keeping the error" do
        calls = 0
        worker = build_worker(new_queue) { synchronized { calls += 1 }; raise ArgumentError, "always broken" }

        run_workers(worker) do
          producer.enqueue("poison")
          wait_until { producer.dead_size == 1 }
        end

        dead = producer.dead_messages.first
        expect(calls).to eq(3)
        expect(dead.raw).to eq("poison")
        expect(dead.reason).to eq("max_attempts")
        expect(dead.attempt).to eq(3)
        expect(dead.error_class).to eq("ArgumentError")
        expect(dead.error_message).to eq("always broken")
        expect(producer.stats).to include(pending: 0, deferred: 0)
      end

      it "can put a dead message back on the queue once the bug is fixed" do
        fixed = false
        worker = build_worker(new_queue) { |_m| fixed ? :done : raise("not yet") }
        handled = []
        Queekiq.subscribe("process.queekiq") { |_, payload| handled << payload[:outcome] }

        run_workers(worker) do
          producer.enqueue("job")
          wait_until { producer.dead_size == 1 }
          fixed = true
          producer.requeue_dead(producer.dead_messages.first.id)
          wait_until { handled.include?(:done) }
        end

        expect(handled).to include(:done)
        expect(producer.dead_size).to eq(0)
      end
    end
  end

  describe "crashed consumers" do
    it "recovers a message a crashed consumer never acked, as a later attempt" do
      producer.enqueue("orphan")
      producer.read(consumer: "dead-consumer", block_ms: 100) # delivered, then the process "dies"
      handled = []
      worker = build_worker(new_queue) { |m| synchronized { handled << [ m.payload, m.attempt ] }; :done }

      run_workers(worker) { wait_until { synchronized { handled.any? } } }

      expect(handled).to eq([ [ "orphan", 2 ] ])
    end

    context "with max_attempts: 2" do
      let(:queue_options) { { max_attempts: 2 } }

      it "dead-letters a message whose workers keep dying, without running the handler" do
        producer.enqueue("crashy")
        crashing = new_queue(reclaim_after_ms: 20)
        crashing.read(consumer: "dead-1", block_ms: 100) # attempt 1
        sleep 0.05
        crashing.reclaim(consumer: "dead-2")             # attempt 2
        sleep 0.05
        calls = 0
        worker = build_worker(new_queue(reclaim_after_ms: 20)) { synchronized { calls += 1 }; :done } # would be attempt 3

        run_workers(worker) { wait_until { producer.dead_size == 1 } }

        expect(calls).to eq(0)
        expect(producer.dead_messages.first).to have_attributes(raw: "crashy", reason: "max_attempts", attempt: 3)
      end
    end
  end

  describe "long-running handlers" do
    let(:queue_options) { { reclaim_after_ms: 300 } }

    def run_long_job(heartbeat)
      calls = []
      handler = lambda do |m|
        synchronized { calls << m.attempt }
        sleep 1.0
        :done
      end
      # Whichever worker reads the message first handles it; the other one waits
      # to reclaim it. Both get the same heartbeat setting.
      long = build_worker(new_queue, "first", heartbeat_interval_ms: heartbeat, &handler)
      rival = build_worker(new_queue, "second", heartbeat_interval_ms: heartbeat, &handler)

      run_workers(long, rival) do
        producer.enqueue("long-job")
        wait_until { synchronized { calls.any? } }
        sleep 1.4
      end
      calls
    end

    it "is not reclaimed by another worker while the heartbeat runs, even past the reclaim window" do
      expect(run_long_job(nil)).to eq([ 1 ])
    end

    it "is handled twice without the heartbeat, which is the at-least-once guarantee at work" do
      expect(run_long_job(false).size).to be >= 2
    end
  end

  describe "shutdown" do
    it "finishes the message in hand before stopping" do
      finished = []
      started = false
      worker = build_worker(new_queue) do |m|
        started = true
        sleep 0.3
        synchronized { finished << m.payload }
        :done
      end

      thread = Thread.new { worker.run }
      producer.enqueue("in-hand")
      wait_until { started }
      worker.stop
      thread.join(5)

      expect(finished).to eq([ "in-hand" ])
      expect(producer.stats[:pending]).to eq(0)
    end

    it "abandons a message that outlives shutdown_timeout, leaving it for another worker" do
      started = false
      worker = build_worker(new_queue, shutdown_timeout: 0.2) { started = true; sleep 10 }

      thread = Thread.new { worker.run }
      producer.enqueue("stuck")
      wait_until { started }
      began = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      worker.stop
      thread.join(3)

      expect(thread).not_to be_alive
      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - began).to be < 2
      expect(producer.stats[:pending]).to eq(1)

      sleep 0.1
      reclaimed = producer.reclaim(consumer: "rescuer")
      expect(reclaimed.map { |m| [ m.raw, m.attempt ] }).to eq([ [ "stuck", 2 ] ])
    end

    it "stops on a signal, abandoning stuck work once shutdown_timeout passes" do
      started = false
      worker = build_worker(new_queue, shutdown_timeout: 0.2) { started = true; sleep 10 }
      producer.enqueue("stuck")
      Thread.new do
        wait_until { started }
        Process.kill("USR1", Process.pid)
      end

      begin
        Timeout.timeout(5) { worker.start(signals: %w[USR1]) }
      ensure
        trap("USR1", "DEFAULT")
      end

      expect(producer.stats[:pending]).to eq(1)
    end
  end

  describe "losing Redis" do
    let(:reconnecting) { { reconnect_backoff: Queekiq::Backoff.fixed(10) } }

    it "keeps working after the connection fails for a while" do
      handled = []
      queue = new_queue
      failures = 0
      allow(queue).to receive(:promote_due!).and_wrap_original do |original, **args|
        failures += 1
        raise Redis::CannotConnectError, "simulated outage" if failures <= 3

        original.call(**args)
      end
      worker = build_worker(queue, **reconnecting) { |m| synchronized { handled << m.payload }; :done }

      run_workers(worker) do
        producer.enqueue("after-outage")
        wait_until { synchronized { handled.any? } }
      end

      expect(failures).to be > 3
      expect(handled).to eq([ "after-outage" ])
    end

    it "recreates the stream and group if Redis came back empty" do
      handled = []
      worker = build_worker(new_queue, **reconnecting) { |m| synchronized { handled << m.payload }; :done }

      run_workers(worker) do
        sleep 0.2
        producer.clear! # what a restart without persistence does
        producer.enqueue("after-wipe")
        wait_until { synchronized { handled.any? } }
      end

      expect(handled).to eq([ "after-wipe" ])
    end
  end

  describe "observability" do
    it "emits events for the whole life of a failing, then deferred, then completed message" do
      events = []
      Queekiq.subscribe { |name, payload| events << [ name, payload[:outcome] ] if name.end_with?(".queekiq") }
      worker = build_worker(new_queue) do |m|
        raise "boom" if m.attempt == 1 && m.defers.zero?

        m.defers.zero? ? :deferred : :done
      end

      run_workers(worker) do
        producer.enqueue("job")
        wait_until { events.any? { |_, outcome| outcome == :done } }
      end

      names = events.map(&:first)
      expect(names).to include("process.queekiq", "retry.queekiq", "defer.queekiq")
      expect(names.index("retry.queekiq")).to be < names.index("defer.queekiq")
    end

    it "shows queue health through stats while a worker is busy" do
      release = false
      worker = build_worker(new_queue) { wait_until { release }; :done }
      stats = nil

      run_workers(worker) do
        3.times { |i| producer.enqueue("job-#{i}") }
        wait_until { producer.stats[:pending] == 1 }
        stats = producer.stats
        release = true
      end

      expect(stats[:pending]).to eq(1)
      expect(stats[:backlog]).to eq(2)
      expect(stats[:consumers].map { |c| c[:name] }).to eq([ "w1" ])
    end
  end
end
