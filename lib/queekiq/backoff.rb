module Queekiq
  # Exponential delay with jitter and a cap: base, base*factor, base*factor^2,
  # ... never more than max_ms, each randomly shifted by up to +/- `jitter`
  # (a fraction, 0.2 = 20%).
  #
  # Anything responding to #delay_ms(attempt) can be used where a backoff is
  # expected; Backoff.fixed(ms) gives a constant delay.
  class Backoff
    attr_reader :base_ms, :max_ms, :factor, :jitter

    def self.fixed(ms)
      new(base_ms: ms, max_ms: ms, factor: 1, jitter: 0)
    end

    def initialize(base_ms: 200, max_ms: 30_000, factor: 2, jitter: 0.2, random: Random)
      @base_ms = base_ms
      @max_ms = max_ms
      @factor = factor.to_f
      @jitter = jitter
      @random = random
    end

    # attempt is 1-based: delay_ms(1) is the delay before the first retry.
    def delay_ms(attempt)
      steps = [ attempt.to_i - 1, 0 ].max
      delay = [ base_ms * (factor**steps), max_ms ].min
      delay *= 1 + (((@random.rand * 2) - 1) * jitter) if jitter.positive?
      [ delay.round, 0 ].max
    end
  end
end
