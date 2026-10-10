require "spec_helper"

RSpec.describe Queekiq::Queue do
  let(:name) { "spec-#{SecureRandom.hex(4)}" }
  let(:options) { {} }
  let(:queue) { described_class.new(name, **options) }
  let(:no_jitter) { Queekiq::Backoff.new(base_ms: 100, max_ms: 10_000, factor: 2, jitter: 0) }

  after { queue.clear! }

  # Delivers one message to "c1" (creating the group first).
  def deliver(payload = "job")
    queue.ensure_group!
    queue.enqueue(payload)
    queue.read(consumer: "c1", block_ms: 200).first
  end

  # Makes everything in the deferred set due and promotes it.
  def promote_now
    queue.redis.zadd(queue.deferred_key, 0, queue.redis.zrange(queue.deferred_key, 0, -1).first)
    queue.promote_due!
  end

  describe "#initialize" do
    it "derives hash-tagged key names from the queue name" do
      expect(queue.stream).to eq("queekiq:{#{name}}:stream")
      expect(queue.deferred_key).to eq("queekiq:{#{name}}:deferred")
      expect(queue.dead_key).to eq("queekiq:{#{name}}:dead")
      expect(queue.group).to eq("queekiq-#{name}")
    end

    it "lets every name be overridden" do
      custom = described_class.new("x", stream: "s", group: "g", deferred_key: "d", dead_key: "dead", field: "f")

      expect([ custom.stream, custom.group, custom.deferred_key, custom.dead_key, custom.field ])
        .to eq(%w[s g d dead f])
    end

    it "rejects a blank name" do
      expect { described_class.new(" ") }.to raise_error(ArgumentError)
    end

    it "rejects a payload field that would clash with the envelope" do
      %w[attempt defers enqueued_at].each do |reserved|
        expect { described_class.new("x", field: reserved) }.to raise_error(ArgumentError, /field/)
      end
    end

    it "uses the globally configured Redis client unless given one" do
      expect(queue.redis).to equal(Queekiq.redis)

      other = Redis.new
      expect(described_class.new("y", redis: other).redis).to equal(other)
    end
  end

  describe "#ensure_group!" do
    it "creates the consumer group so read doesn't raise" do
      expect { queue.ensure_group! }.not_to raise_error
      expect { queue.read(consumer: "c1", block_ms: 50) }.not_to raise_error
    end

    it "is idempotent" do
      queue.ensure_group!

      expect { queue.ensure_group! }.not_to raise_error
    end

    it "starts the group at the beginning of the stream, not just new entries" do
      queue.enqueue("written-before-the-group")

      queue.ensure_group!

      expect(queue.read(consumer: "c1", block_ms: 200).map(&:raw)).to eq([ "written-before-the-group" ])
    end
  end

  describe "#enqueue / #read" do
    before { queue.ensure_group! }

    it "delivers an enqueued payload as a Message with a real stream id" do
      queue.enqueue("job-1")

      messages = queue.read(consumer: "c1", block_ms: 200)

      expect(messages.size).to eq(1)
      expect(messages.first.payload).to eq("job-1")
      expect(messages.first.id).to match(/\A\d+-\d+\z/)
    end

    it "returns the stream entry id from #enqueue" do
      expect(queue.enqueue("job-1")).to match(/\A\d+-\d+\z/)
    end

    it "stamps the message with an envelope: first attempt, no defers, enqueue time" do
      before = (Time.now.to_f * 1000).to_i
      queue.enqueue("job-1")

      message = queue.read(consumer: "c1", block_ms: 200).first

      expect(message.attempt).to eq(1)
      expect(message.defers).to eq(0)
      expect(message.enqueued_at).to be_between(before, (Time.now.to_f * 1000).to_i)
    end

    it "stores non-string payloads as strings with the default serializer" do
      queue.enqueue(42)

      expect(queue.read(consumer: "c1", block_ms: 200).first.payload).to eq("42")
    end

    it "respects count when several messages are available" do
      3.times { |i| queue.enqueue("job-#{i}") }

      expect(queue.read(consumer: "c1", count: 2, block_ms: 200).size).to eq(2)
    end

    it "returns an empty array (not nil) when nothing is available" do
      expect(queue.read(consumer: "c1", block_ms: 50)).to eq([])
    end

    it "never delivers the same message to two different consumers" do
      payloads = %w[a b]
      payloads.each { |p| queue.enqueue(p) }

      from_a = queue.read(consumer: "consumer-a", block_ms: 200).map(&:raw)
      from_b = queue.read(consumer: "consumer-b", block_ms: 200).map(&:raw)

      expect(from_a & from_b).to be_empty
      expect((from_a + from_b).sort).to eq(payloads)
    end

    it "reads from a custom payload field" do
      custom = described_class.new(name, field: "battle_id")
      custom.ensure_group!
      custom.enqueue("abc")

      expect(custom.read(consumer: "c1", block_ms: 200).first.payload).to eq("abc")
    end

    it "reads entries that only have the payload field, like the ones Queekiq 0.1 wrote" do
      queue.redis.xadd(queue.stream, { "payload" => "legacy" })

      message = queue.read(consumer: "c1", block_ms: 200).first

      expect(message.payload).to eq("legacy")
      expect([ message.attempt, message.defers, message.enqueued_at ]).to eq([ 1, 0, nil ])
    end

    it "raises when an entry lacks the payload field" do
      queue.redis.xadd(queue.stream, { "something_else" => "x" })

      expect { queue.read(consumer: "c1", block_ms: 200) }.to raise_error(Queekiq::Error, /no "payload" field/)
    end

    context "with the JSON serializer" do
      let(:options) { { serializer: Queekiq::Serializers::JSON } }

      it "round-trips structured payloads" do
        queue.ensure_group!
        queue.enqueue("order_id" => 7, "items" => [ 1, 2 ])

        message = queue.read(consumer: "c1", block_ms: 200).first

        expect(message.payload).to eq("order_id" => 7, "items" => [ 1, 2 ])
        expect(message.raw).to eq('{"order_id":7,"items":[1,2]}')
      end

      it "delivers an undecodable payload and only fails when it is used" do
        queue.ensure_group!
        queue.redis.xadd(queue.stream, { "payload" => "not-json" })

        message = queue.read(consumer: "c1", block_ms: 200).first

        expect(message.raw).to eq("not-json")
        expect { message.payload }.to raise_error(Queekiq::SerializationError)
      end
    end
  end

  describe "#ack" do
    let(:options) { { reclaim_after_ms: 20 } }

    before { queue.ensure_group! }

    it "removes the message from the pending list, so it isn't reclaimable afterwards" do
      queue.enqueue("job-1")
      message = queue.read(consumer: "c1", block_ms: 200).first

      queue.ack(message)
      sleep 0.05

      expect(queue.reclaim(consumer: "c2")).to be_empty
    end

    it "accepts a bare entry id" do
      queue.enqueue("job-1")
      message = queue.read(consumer: "c1", block_ms: 200).first

      queue.ack(message.id)
      sleep 0.05

      expect(queue.reclaim(consumer: "c2")).to be_empty
    end

    it "is a no-op for an unknown id" do
      expect { queue.ack("1-1") }.not_to raise_error
    end

    it "keeps acked entries in the stream by default" do
      queue.enqueue("job-1")
      queue.ack(queue.read(consumer: "c1", block_ms: 200).first)

      expect(queue.size).to eq(1)
    end

    context "with delete_on_ack" do
      let(:options) { { delete_on_ack: true } }

      it "removes the entry from the stream too" do
        queue.enqueue("job-1")
        queue.ack(queue.read(consumer: "c1", block_ms: 200).first)

        expect(queue.size).to eq(0)
      end
    end
  end

  describe "#reclaim" do
    let(:options) { { reclaim_after_ms: 20 } }

    before { queue.ensure_group! }

    it "returns an empty array when nothing has been delivered" do
      expect(queue.reclaim(consumer: "c1")).to eq([])
    end

    it "does not reclaim a message that was delivered too recently" do
      slow = described_class.new(name, reclaim_after_ms: 60_000)
      slow.enqueue("job-1")
      slow.read(consumer: "c1", block_ms: 200) # delivered, not acked

      expect(slow.reclaim(consumer: "c2")).to eq([])
    end

    it "reclaims a message whose consumer stalled past the window" do
      queue.enqueue("job-1")
      queue.read(consumer: "c1", block_ms: 200) # delivered, never acked
      sleep 0.05

      expect(queue.reclaim(consumer: "c2").map(&:raw)).to eq([ "job-1" ])
    end

    it "counts every delivery in the message's attempt" do
      queue.enqueue("job-1")
      queue.read(consumer: "c1", block_ms: 200)
      sleep 0.05
      second = queue.reclaim(consumer: "c2").first
      sleep 0.05
      third = queue.reclaim(consumer: "c3").first

      expect([ second.attempt, third.attempt ]).to eq([ 2, 3 ])
    end

    it "hands ownership to the claiming consumer, so it isn't claimed twice at once" do
      slow = described_class.new(name, reclaim_after_ms: 100)
      slow.enqueue("job-1")
      slow.read(consumer: "c1", block_ms: 200)
      sleep 0.15
      slow.reclaim(consumer: "c2")

      # c2's claim reset the idle time, so an immediate second claim finds nothing.
      expect(slow.reclaim(consumer: "c3")).to eq([])
    end
  end

  describe "#touch" do
    let(:options) { { reclaim_after_ms: 150 } }

    it "keeps a message from being reclaimed while its consumer keeps touching it" do
      message = deliver
      4.times do
        sleep 0.08
        expect(queue.touch(message, consumer: "c1")).to be(true)
      end

      expect(queue.reclaim(consumer: "c2")).to be_empty
    end

    it "does not reset the delivery count" do
      message = deliver
      queue.touch(message, consumer: "c1")
      sleep 0.2

      expect(queue.reclaim(consumer: "c2").first.attempt).to eq(2)
    end

    it "returns false for a consumer that doesn't own the message (it can't steal it back)" do
      message = deliver
      sleep 0.2
      queue.reclaim(consumer: "c2")

      expect(queue.touch(message, consumer: "c1")).to be(false)
      expect(queue.touch(message, consumer: "c2")).to be(true)
    end

    it "returns false once the message is acked" do
      message = deliver
      queue.ack(message)

      expect(queue.touch(message, consumer: "c1")).to be(false)
    end

    it "accepts a bare entry id" do
      message = deliver

      expect(queue.touch(message.id, consumer: "c1")).to be(true)
    end
  end

  describe "#defer / #promote_due!" do
    let(:options) { { defer_backoff: no_jitter } }

    it "does not promote before the delay elapses" do
      queue.defer("job-x", delay_ms: 60_000)

      expect(queue.promote_due!).to eq(0)
      expect(queue.deferred_size).to eq(1)
    end

    it "returns the delay it used" do
      expect(queue.defer("job-x", delay_ms: 5_000)).to eq(5_000)
      expect(queue.defer("job-y")).to eq(100)
    end

    it "re-enqueues onto the stream once the delay elapses" do
      queue.ensure_group!
      queue.defer("job-y", delay_ms: 0)

      expect(queue.promote_due!).to eq(1)
      expect(queue.deferred_size).to eq(0)
      expect(queue.read(consumer: "c1", block_ms: 200).map(&:raw)).to eq([ "job-y" ])
    end

    it "promotes an entry only once even if called twice" do
      queue.defer("job-z", delay_ms: 0)

      expect(queue.promote_due!).to eq(1)
      expect(queue.promote_due!).to eq(0)
      expect(queue.size).to eq(1)
    end

    it "promotes up to limit, leaving the remainder for the next call" do
      3.times { |i| queue.defer("job-#{i}", delay_ms: 0) }

      expect(queue.promote_due!(limit: 2)).to eq(2)
      expect(queue.promote_due!).to eq(1)
    end

    it "promotes onto the configured payload field" do
      custom = described_class.new(name, field: "battle_id")
      custom.ensure_group!
      custom.defer("abc", delay_ms: 0)
      custom.promote_due!

      expect(custom.read(consumer: "c1", block_ms: 200).first.raw).to eq("abc")
    end

    it "promotes plain members the way Queekiq 0.1 stored them" do
      queue.ensure_group!
      queue.redis.zadd(queue.deferred_key, 0, "plain-payload")

      queue.promote_due!

      message = queue.read(consumer: "c1", block_ms: 200).first
      expect(message.raw).to eq("plain-payload")
      expect([ message.attempt, message.defers ]).to eq([ 1, 0 ])
      expect(message.enqueued_at).to be_a(Integer)
    end

    it "keeps payloads containing the member separator or newlines intact" do
      queue.ensure_group!
      tricky = "1|2|3|a|b\nline two"
      queue.defer(tricky, delay_ms: 0)
      queue.promote_due!

      expect(queue.read(consumer: "c1", block_ms: 200).first.raw).to eq(tricky)
    end

    context "when deferring a delivered message" do
      it "acks the delivery in the same step, so it is no longer pending" do
        message = deliver
        queue.defer(message, delay_ms: 60_000)

        expect(queue.stats[:pending]).to eq(0)
        expect(queue.deferred_size).to eq(1)
      end

      it "brings it back with its history: same attempt, one more defer, original enqueue time" do
        message = deliver
        queue.defer(message, delay_ms: 0)
        queue.promote_due!

        again = queue.read(consumer: "c1", block_ms: 200).first

        expect(again.raw).to eq("job")
        expect(again.attempt).to eq(message.attempt)
        expect(again.defers).to eq(1)
        expect(again.enqueued_at).to eq(message.enqueued_at)
        expect(again.id).not_to eq(message.id)
      end

      it "backs off further each time it is deferred" do
        message = deliver
        delays = Array.new(4) do
          delay = queue.defer(message, delay_ms: nil)
          promote_now
          message = queue.read(consumer: "c1", block_ms: 200).first
          delay
        end

        expect(delays).to eq([ 100, 200, 400, 800 ])
      end

      context "with delete_on_ack" do
        let(:options) { { defer_backoff: no_jitter, delete_on_ack: true } }

        it "also removes the old entry from the stream" do
          message = deliver
          queue.defer(message, delay_ms: 60_000)

          expect(queue.size).to eq(0)
        end
      end
    end
  end

  describe "#retry_later" do
    let(:options) { { retry_backoff: no_jitter } }

    it "schedules another attempt and acks the current delivery" do
      message = deliver
      queue.retry_later(message, delay_ms: 60_000)

      expect(queue.stats[:pending]).to eq(0)
      expect(queue.deferred_size).to eq(1)
    end

    it "bumps the attempt when the message comes back, keeping its other history" do
      message = deliver
      queue.retry_later(message, delay_ms: 0)
      queue.promote_due!

      again = queue.read(consumer: "c1", block_ms: 200).first

      expect(again.attempt).to eq(2)
      expect(again.defers).to eq(0)
      expect(again.enqueued_at).to eq(message.enqueued_at)
    end

    it "uses retry_backoff by attempt unless a delay is given" do
      message = deliver

      expect(queue.retry_later(message)).to eq(100)
    end

    it "grows the delay with each failed attempt" do
      message = deliver
      delays = Array.new(3) do
        delay = queue.retry_later(message)
        promote_now
        message = queue.read(consumer: "c1", block_ms: 200).first
        delay
      end

      expect(delays).to eq([ 100, 200, 400 ])
    end
  end

  describe "attempt and defer limits" do
    let(:options) { { max_attempts: 3, max_defers: 2 } }

    def message_with(attempt: 1, defers: 0)
      Queekiq::Message.new(id: "1-0", payload: "x", attempt: attempt, defers: defers)
    end

    it "flags the last allowed attempt" do
      expect([ 1, 2, 3, 4 ].map { |n| queue.last_attempt?(message_with(attempt: n)) })
        .to eq([ false, false, true, true ])
    end

    it "flags a message that has gone past its attempts (e.g. it keeps crashing workers)" do
      expect([ 3, 4 ].map { |n| queue.attempts_exhausted?(message_with(attempt: n)) }).to eq([ false, true ])
    end

    it "flags a message deferred max_defers times already" do
      expect([ 0, 1, 2 ].map { |n| queue.defers_exhausted?(message_with(defers: n)) })
        .to eq([ false, false, true ])
    end

    it "defaults to 5 attempts and unlimited defers" do
      default = described_class.new("z")

      expect(default.max_attempts).to eq(5)
      expect(default.defers_exhausted?(message_with(defers: 1_000))).to be(false)
    end

    it "means unlimited when nil" do
      unlimited = described_class.new("z", max_attempts: nil)

      expect(unlimited.last_attempt?(message_with(attempt: 1_000))).to be(false)
      expect(unlimited.attempts_exhausted?(message_with(attempt: 1_000))).to be(false)
    end
  end

  describe "dead letters" do
    let(:error) { RuntimeError.new("it broke") }

    it "moves a message to the dead stream with the reason and error, and acks it" do
      message = deliver("job-dead")

      queue.dead_letter(message, reason: :max_attempts, error: error)

      dead = queue.dead_messages.first
      expect(queue.dead_size).to eq(1)
      expect(queue.stats[:pending]).to eq(0)
      expect(dead.raw).to eq("job-dead")
      expect(dead.reason).to eq("max_attempts")
      expect(dead.error_class).to eq("RuntimeError")
      expect(dead.error_message).to eq("it broke")
      expect(dead.attempt).to eq(1)
      expect(dead.original_id).to eq(message.id)
      expect(dead.enqueued_at).to eq(message.enqueued_at)
      expect(dead.dead_at).to be_a(Integer)
    end

    it "works without an error" do
      queue.dead_letter(deliver, reason: :max_defers)

      dead = queue.dead_messages.first
      expect(dead.reason).to eq("max_defers")
      expect(dead.error_class).to be_nil
    end

    it "truncates very long error messages" do
      queue.dead_letter(deliver, reason: :max_attempts, error: RuntimeError.new("x" * 5_000))

      expect(queue.dead_messages.first.error_message.size).to eq(1_000)
    end

    it "keeps the payload field name of the queue" do
      custom = described_class.new(name, field: "battle_id")
      custom.dead_letter(Queekiq::Message.new(id: nil, payload: "abc"), reason: :test)

      expect(custom.dead_messages.first.raw).to eq("abc")
    end

    it "requeues a dead message with a fresh attempt counter" do
      message = deliver("job-dead")
      queue.dead_letter(message, reason: :max_attempts, error: error)
      dead_id = queue.dead_messages.first.id

      expect(queue.requeue_dead(dead_id)).to be(true)

      again = queue.read(consumer: "c1", block_ms: 200).first
      expect(again.raw).to eq("job-dead")
      expect(again.attempt).to eq(1)
      expect(queue.dead_size).to eq(0)
    end

    it "reports false when asked to requeue an unknown dead message" do
      expect(queue.requeue_dead("1-1")).to be(false)
    end

    it "deletes a dead message" do
      queue.dead_letter(deliver, reason: :test)
      dead_id = queue.dead_messages.first.id

      expect(queue.delete_dead(dead_id)).to be(true)
      expect(queue.delete_dead(dead_id)).to be(false)
      expect(queue.dead_size).to eq(0)
    end

    it "limits how many dead messages are listed" do
      3.times { |i| queue.dead_letter(Queekiq::Message.new(id: nil, payload: "m#{i}"), reason: :test) }

      expect(queue.dead_messages(count: 2).map(&:raw)).to eq(%w[m0 m1])
    end
  end

  describe "#stats" do
    it "is all zeroes before the queue has ever been used" do
      expect(queue.stats).to eq(backlog: nil, pending: 0, oldest_pending_idle_ms: nil,
                                deferred: 0, dead: 0, consumers: [])
    end

    it "reports backlog, pending, deferred and dead counts" do
      queue.ensure_group!
      3.times { |i| queue.enqueue("job-#{i}") }
      queue.read(consumer: "c1", count: 2, block_ms: 200)
      queue.defer("later", delay_ms: 60_000)
      queue.dead_letter(Queekiq::Message.new(id: nil, payload: "gone"), reason: :test)

      stats = queue.stats

      expect(stats[:backlog]).to eq(1)
      expect(stats[:pending]).to eq(2)
      expect(stats[:deferred]).to eq(1)
      expect(stats[:dead]).to eq(1)
    end

    it "shows how long the oldest pending message has been idle" do
      deliver
      sleep 0.1

      expect(queue.stats[:oldest_pending_idle_ms]).to be >= 100
    end

    it "has no idle time when nothing is pending" do
      queue.ack(deliver)

      expect(queue.stats[:oldest_pending_idle_ms]).to be_nil
    end

    it "lists consumers with their pending counts" do
      queue.ensure_group!
      2.times { |i| queue.enqueue("job-#{i}") }
      queue.read(consumer: "c1", block_ms: 200)
      queue.read(consumer: "c2", block_ms: 200)

      consumers = queue.stats[:consumers]

      expect(consumers.map { |c| c[:name] }).to contain_exactly("c1", "c2")
      expect(consumers).to all(include(pending: 1, idle_ms: be_a(Integer)))
    end
  end

  describe "#clear!" do
    it "removes the stream, the deferred set and the dead stream" do
      message = deliver
      queue.defer("later", delay_ms: 60_000)
      queue.dead_letter(message, reason: :test)

      queue.clear!

      expect([ queue.size, queue.deferred_size, queue.dead_size ]).to eq([ 0, 0, 0 ])
    end
  end
end
