module Queekiq
  # A durable work queue: a Redis Stream read through a consumer group, plus a
  # sorted set of "deferred" payloads that are pushed back onto the stream once
  # their delay has elapsed.
  #
  #   queue = Queekiq::Queue.new("emails")
  #   queue.enqueue("user-42")
  #   queue.ensure_group!
  #   queue.read(consumer: "worker-1").each { |m| ...; queue.ack(m) }
  #
  # Delivery is at-least-once: a message stays pending until it is acked, and
  # #reclaim hands pending messages that have been idle too long to another
  # consumer. Handlers must therefore be idempotent.
  #
  # Requires Redis >= 7.0.
  class Queue
    DEFAULT_FIELD = "payload".freeze
    DEFAULT_RECLAIM_AFTER_MS = 30_000
    DEFAULT_DEFER_DELAY_MS = 200

    # Moves due members of the deferred set onto the stream in one atomic step,
    # so a crash can't lose a payload between "removed" and "re-enqueued", and
    # two workers can't both promote the same one.
    #   KEYS: deferred set, stream   ARGV: now_ms, limit, field name
    PROMOTE_SCRIPT = <<~LUA.freeze
      local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
      for _, member in ipairs(due) do
        redis.call('ZREM', KEYS[1], member)
        redis.call('XADD', KEYS[2], '*', ARGV[3], member)
      end
      return #due
    LUA

    attr_reader :name, :stream, :group, :deferred_key, :field, :reclaim_after_ms, :defer_delay_ms

    # name             - used to derive the default Redis keys.
    # stream/group/deferred_key
    #                  - override the derived names, e.g. to adopt keys an
    #                    application already uses. (On Redis Cluster the stream
    #                    and deferred key must share a hash tag.)
    # field            - the stream field holding the payload.
    # reclaim_after_ms - idle time after which a pending message may be reclaimed.
    # defer_delay_ms   - default delay for #defer.
    # delete_on_ack    - also XDEL entries once acked, so the stream doesn't
    #                    grow forever. Only safe with a single consumer group
    #                    (which is what Queekiq uses).
    # redis            - a client for this queue; defaults to Queekiq.redis.
    def initialize(name, stream: nil, group: nil, deferred_key: nil, field: DEFAULT_FIELD,
                   reclaim_after_ms: DEFAULT_RECLAIM_AFTER_MS, defer_delay_ms: DEFAULT_DEFER_DELAY_MS,
                   delete_on_ack: false, redis: nil)
      raise ArgumentError, "queue name can't be blank" if name.to_s.strip.empty?

      @name = name.to_s
      @stream = stream || "queekiq:{#{@name}}:stream"
      @group = group || "queekiq-#{@name}"
      @deferred_key = deferred_key || "queekiq:{#{@name}}:deferred"
      @field = field
      @reclaim_after_ms = reclaim_after_ms
      @defer_delay_ms = defer_delay_ms
      @delete_on_ack = delete_on_ack
      @redis = redis
    end

    def redis
      @redis || Queekiq.redis
    end

    # Appends a message to the stream. `payload` is stored as a string (use
    # JSON yourself for structured data). Returns the stream entry id.
    def enqueue(payload)
      redis.xadd(stream, { field => payload.to_s })
    end

    # Schedules `payload` to be re-enqueued after `delay_ms`. The deferred set
    # holds one entry per distinct payload: deferring the same payload twice
    # just moves its due time.
    def defer(payload, delay_ms: defer_delay_ms)
      redis.zadd(deferred_key, now_ms + delay_ms, payload.to_s)
    end

    # Re-enqueues up to `limit` deferred payloads whose delay has elapsed.
    # Returns how many were promoted.
    def promote_due!(limit: 50)
      redis.eval(PROMOTE_SCRIPT, keys: [ deferred_key, stream ], argv: [ now_ms, limit, field ])
    end

    # Creates the consumer group (and the stream if needed). Safe to call
    # repeatedly. The group starts at the beginning of the stream, so messages
    # enqueued before the group existed are still delivered.
    def ensure_group!
      redis.xgroup(:create, stream, group, "0", mkstream: true)
    rescue Redis::CommandError => e
      raise unless e.message.include?("BUSYGROUP")
    end

    # Blocks up to `block_ms` for new messages for `consumer`.
    # Returns an array of Message (empty when nothing arrived). Messages stay
    # pending until #ack.
    def read(consumer:, count: 1, block_ms: 5_000)
      result = redis.xreadgroup(group, consumer, stream, ">", count: count, block: block_ms)
      build_messages(result&.dig(stream) || [])
    end

    # Takes over pending messages that have been idle for reclaim_after_ms
    # (their consumer probably died) and returns them as Messages.
    def reclaim(consumer:, count: 10)
      result = redis.xautoclaim(stream, group, consumer, reclaim_after_ms, "0-0", count: count)
      entries = result.is_a?(Hash) ? result["entries"] : result[1]
      build_messages(entries || [])
    end

    # Marks a message as done. Accepts a Message or a bare stream entry id.
    def ack(message_or_id)
      id = message_or_id.respond_to?(:id) ? message_or_id.id : message_or_id
      redis.xack(stream, group, id)
      redis.xdel(stream, id) if @delete_on_ack
    end

    # Number of entries currently in the stream.
    def size
      redis.xlen(stream)
    end

    # Number of payloads waiting in the deferred set.
    def deferred_size
      redis.zcard(deferred_key)
    end

    # Deletes the stream (and its group) and the deferred set. Meant for tests.
    def clear!
      redis.del(stream, deferred_key)
    end

    private

    def now_ms
      (Time.now.to_f * 1000).to_i
    end

    def build_messages(entries)
      entries.map do |id, fields|
        payload = fields.fetch(field) { raise Error, "stream entry #{id} has no #{field.inspect} field" }
        Message.new(id: id, payload: payload)
      end
    end
  end
end
