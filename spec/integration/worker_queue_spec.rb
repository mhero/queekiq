require "spec_helper"

# End to end against a real Redis: a Worker running its loop on a Queue.
RSpec.describe "Worker + Queue" do
  let(:name) { "spec-#{SecureRandom.hex(4)}" }
  let(:queue_options) { { reclaim_after_ms: 50, defer_delay_ms: 20 } }

  # One Redis connection per worker: a blocking read occupies its connection.
  def new_queue(**overrides)
    Queekiq::Queue.new(name, redis: Redis.new(url: Queekiq.configuration.redis_url), **queue_options, **overrides)
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

  def build_worker(queue, consumer, **options, &handler)
    Queekiq::Worker.new(queue: queue, consumer: consumer, block_ms: 50, logger: Logger.new(nil), handler: handler, **options)
  end

  after { new_queue.clear! }

  it "processes every message exactly once across competing workers" do
    producer = new_queue
    producer.ensure_group!
    seen = []
    lock = Mutex.new
    handler = ->(m) { lock.synchronize { seen << m.payload }; :done }
    workers = %w[w1 w2].map { |c| build_worker(new_queue, c, &handler) }
    payloads = Array.new(20) { |i| "job-#{i}" }

    run_workers(*workers) do
      payloads.each { |p| producer.enqueue(p) }
      wait_until { lock.synchronize { seen.size >= payloads.size } }
    end

    expect(seen.sort).to eq(payloads.sort)
  end

  it "retries a deferred message until its dependency is satisfied" do
    producer = new_queue
    producer.ensure_group!
    done = []
    lock = Mutex.new
    worker = build_worker(new_queue, "w1") do |m|
      lock.synchronize do
        next :deferred if m.payload == "b" && !done.include?("a")

        done << m.payload
        :done
      end
    end

    run_workers(worker) do
      producer.enqueue("b") # arrives first, but must wait for "a"
      producer.enqueue("a")
      wait_until { lock.synchronize { done.size >= 2 } }
    end

    expect(done).to eq(%w[a b])
  end

  it "redelivers a message after its handler raised, once the reclaim window passes" do
    producer = new_queue
    producer.ensure_group!
    attempts = []
    lock = Mutex.new
    worker = build_worker(new_queue, "w1") do |m|
      lock.synchronize do
        attempts << m.payload
        raise "flaky" if attempts.size == 1

        :done
      end
    end

    run_workers(worker) do
      producer.enqueue("job")
      wait_until { lock.synchronize { attempts.size >= 2 } }
    end

    expect(attempts).to eq(%w[job job])
  end

  it "recovers a message a crashed consumer never acked" do
    producer = new_queue
    producer.ensure_group!
    producer.enqueue("orphan")
    producer.read(consumer: "dead-consumer", block_ms: 100) # delivered, then the process "dies"
    handled = []
    lock = Mutex.new
    worker = build_worker(new_queue, "survivor") { |m| lock.synchronize { handled << m.payload }; :done }

    run_workers(worker) { wait_until { lock.synchronize { handled.any? } } }

    expect(handled).to eq([ "orphan" ])
  end
end
