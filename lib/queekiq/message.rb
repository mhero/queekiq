module Queekiq
  # One delivery from a queue.
  #
  # id          - the Redis stream entry id (needed to ack); nil for a message
  #               that was never delivered.
  # payload     - the enqueued object, decoded with the queue's serializer. It is
  #               decoded on first use, so a payload that can't be decoded fails
  #               inside the handler (and is retried / dead-lettered) instead of
  #               crashing the worker.
  # raw         - the payload exactly as stored in Redis (a String).
  # attempt     - 1 for the first delivery; goes up each time the message is
  #               redelivered after a failure or after its worker died.
  # defers      - how many times the message has been deferred so far.
  # enqueued_at - when it was first enqueued, in epoch milliseconds (nil for
  #               entries written by Queekiq 0.1).
  class Message
    NOT_LOADED = Object.new.freeze
    private_constant :NOT_LOADED

    attr_reader :id, :raw, :attempt, :defers, :enqueued_at

    # Pass `payload:` (it is serialized to get `raw`) or `raw:` (decoded lazily).
    def initialize(id:, payload: NOT_LOADED, raw: nil, serializer: Serializers::Raw,
                   attempt: 1, defers: 0, enqueued_at: nil)
      raise ArgumentError, "pass payload: or raw:" if payload.equal?(NOT_LOADED) && raw.nil?

      @id = id
      @serializer = serializer
      @payload = payload
      @raw = raw.nil? ? serializer.dump(payload) : raw
      @attempt = attempt
      @defers = defers
      @enqueued_at = enqueued_at
    end

    def payload
      @payload = @serializer.load(@raw) if @payload.equal?(NOT_LOADED)
      @payload
    end

    # Milliseconds since the message was first enqueued, or nil if unknown.
    def latency_ms(now_ms = (Time.now.to_f * 1000).to_i)
      now_ms - enqueued_at if enqueued_at
    end

    def inspect
      "#<#{self.class} id=#{id.inspect} raw=#{raw[0, 60].inspect} attempt=#{attempt} defers=#{defers}>"
    end
  end
end
