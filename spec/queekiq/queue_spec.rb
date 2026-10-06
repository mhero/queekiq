require "spec_helper"

RSpec.describe Queekiq::Queue do
  let(:name) { "spec-#{SecureRandom.hex(4)}" }
  let(:options) { {} }
  let(:queue) { described_class.new(name, **options) }

  after { queue.clear! }

  describe "#initialize" do
    it "derives hash-tagged key names from the queue name" do
      expect(queue.stream).to eq("queekiq:{#{name}}:stream")
      expect(queue.deferred_key).to eq("queekiq:{#{name}}:deferred")
      expect(queue.group).to eq("queekiq-#{name}")
    end

    it "lets every name be overridden" do
      custom = described_class.new("x", stream: "s", group: "g", deferred_key: "d", field: "f")

      expect([ custom.stream, custom.group, custom.deferred_key, custom.field ]).to eq(%w[s g d f])
    end

    it "rejects a blank name" do
      expect { described_class.new(" ") }.to raise_error(ArgumentError)
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

      expect(queue.read(consumer: "c1", block_ms: 200).map(&:payload)).to eq([ "written-before-the-group" ])
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

    it "stores non-string payloads as strings" do
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

      from_a = queue.read(consumer: "consumer-a", block_ms: 200).map(&:payload)
      from_b = queue.read(consumer: "consumer-b", block_ms: 200).map(&:payload)

      expect(from_a & from_b).to be_empty
      expect((from_a + from_b).sort).to eq(payloads)
    end

    it "reads from a custom payload field" do
      custom = described_class.new(name, field: "battle_id")
      custom.ensure_group!
      custom.enqueue("abc")

      expect(custom.read(consumer: "c1", block_ms: 200).first.payload).to eq("abc")
    end

    it "raises when an entry lacks the payload field" do
      queue.redis.xadd(queue.stream, { "something_else" => "x" })

      expect { queue.read(consumer: "c1", block_ms: 200) }.to raise_error(Queekiq::Error, /no "payload" field/)
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

      expect(queue.reclaim(consumer: "c2").map(&:payload)).to eq([ "job-1" ])
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

  describe "#defer / #promote_due!" do
    it "does not promote before the delay elapses" do
      queue.defer("job-x", delay_ms: 60_000)

      expect(queue.promote_due!).to eq(0)
      expect(queue.deferred_size).to eq(1)
    end

    it "uses defer_delay_ms when no delay is given" do
      slow = described_class.new(name, defer_delay_ms: 60_000)
      slow.defer("job-x")

      expect(slow.promote_due!).to eq(0)
    end

    it "re-enqueues onto the stream once the delay elapses" do
      queue.ensure_group!
      queue.defer("job-y", delay_ms: 0)

      expect(queue.promote_due!).to eq(1)
      expect(queue.deferred_size).to eq(0)
      expect(queue.read(consumer: "c1", block_ms: 200).map(&:payload)).to eq([ "job-y" ])
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

      expect(custom.read(consumer: "c1", block_ms: 200).first.payload).to eq("abc")
    end
  end
end
