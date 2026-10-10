require "spec_helper"

RSpec.describe Queekiq::Backoff do
  describe "#delay_ms" do
    subject(:backoff) { described_class.new(base_ms: 100, max_ms: 1_000, factor: 2, jitter: 0) }

    it "grows exponentially from the base" do
      expect((1..4).map { |n| backoff.delay_ms(n) }).to eq([ 100, 200, 400, 800 ])
    end

    it "never exceeds the cap, even for huge attempt numbers" do
      expect(backoff.delay_ms(5)).to eq(1_000)
      expect(backoff.delay_ms(10_000)).to eq(1_000)
    end

    it "treats attempt 0 or negative like the first attempt" do
      expect(backoff.delay_ms(0)).to eq(100)
      expect(backoff.delay_ms(-3)).to eq(100)
    end

    it "returns whole milliseconds" do
      fractional = described_class.new(base_ms: 100, max_ms: 10_000, factor: 1.5, jitter: 0)

      expect(fractional.delay_ms(3)).to eq(225)
      expect(fractional.delay_ms(3)).to be_a(Integer)
    end
  end

  describe "jitter" do
    it "shifts the delay by at most +/- the given fraction" do
      backoff = described_class.new(base_ms: 1_000, max_ms: 1_000, jitter: 0.2)

      delays = Array.new(200) { backoff.delay_ms(1) }

      expect(delays).to all(be_between(800, 1_200))
      expect(delays.uniq.size).to be > 1
    end

    it "is deterministic with an injected random source" do
      low = instance_double(Random, rand: 0.0)
      high = instance_double(Random, rand: 1.0)

      expect(described_class.new(base_ms: 1_000, jitter: 0.2, random: low).delay_ms(1)).to eq(800)
      expect(described_class.new(base_ms: 1_000, jitter: 0.2, random: high).delay_ms(1)).to eq(1_200)
    end
  end

  describe ".fixed" do
    it "always returns the same delay" do
      fixed = described_class.fixed(250)

      expect((1..5).map { |n| fixed.delay_ms(n) }).to all(eq(250))
    end
  end
end
