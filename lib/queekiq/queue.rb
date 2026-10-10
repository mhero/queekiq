module Queekiq
  # A durable work queue: a Redis Stream read through a consumer group, a sorted
  # set of "deferred" messages that come back once their delay has elapsed, and
  # a "dead" stream for messages that gave up.
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
  # Besides the payload, every entry carries a small envelope (attempt, defers,
  # enqueued_at) so retries and deferrals keep their history. Entries written by
  # Queekiq 0.1, or by anything else that only sets the payload field, are read
  # as attempt 1 with no history.
  #
  # Requires Redis >= 7.0.
  class Queue
    DEFAULT_FIELD = "payload".freeze
    DEFAULT_RECLAIM_AFTER_MS = 30_000
    DEFAULT_MAX_ATTEMPTS = 5
    RESERVED_FIELDS = %w[attempt defers enqueued_at].freeze

    # Moves due members of the deferred set onto the stream in one atomic step,
    # so a crash can't lose a message between "removed" and "re-enqueued", and
    # two workers can't both promote the same one.
    # Members look like "attempt|defers|enqueued_at|raw payload". A member that
    # doesn't match (written by 0.1: just the payload) is promoted as a new entry.
    #   KEYS: deferred set, stream   ARGV: now_ms, limit, field name
    PROMOTE_SCRIPT = <<~LUA.freeze
      local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
      for _, member in ipairs(due) do
        redis.call('ZREM', KEYS[1], member)
        local attempt, defers, enqueued_at, raw = string.match(member, '^(%d+)|(%d+)|(%d+)|(.*)$')
        if raw then
          redis.call('XADD', KEYS[2], '*', ARGV[3], raw, 'attempt', attempt, 'defers', defers, 'enqueued_at', enqueued_at)
        else
          redis.call('XADD', KEYS[2], '*', ARGV[3], member, 'enqueued_at', ARGV[1])
        end
      end
      return #due
    LUA

    # Resets a pending message's idle timer, but only while it still belongs to
    # the given consumer, so a worker that stalled and was reclaimed can't steal
    # the message back. Returns 1 if it did, 0 if not.
    #   KEYS: stream   ARGV: group, consumer, entry id
    TOUCH_SCRIPT = <<~LUA.freeze
      local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
      if pending[1] and pending[1][2] == ARGV[2] then
        redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
        return 1
      end
      return 0
    LUA

    # A message in the dead stream. `raw` is the stored payload (not decoded).
    DeadMessage = Struct.new(:id, :raw, :reason, :error_class, :error_message,
                             :attempt, :defers, :enqueued_at, :dead_at, :original_id, keyword_init: true)

    attr_reader :name, :stream, :group, :deferred_key, :dead_key, :field, :serializer,
                :reclaim_after_ms, :max_attempts, :max_defers, :retry_backoff, :defer_backoff

    # name             - used to derive the default Redis keys.
    # stream/group/deferred_key/dead_key
    #                  - override the derived names, e.g. to adopt keys an
    #                    application already uses. (On Redis Cluster the stream,
    #                    deferred and dead keys must share a hash tag.)
    # field            - the stream field holding the payload.
    # serializer       - see Queekiq::Serializers (default: Raw, plain strings).
    # reclaim_after_ms - idle time after which a pending message may be reclaimed.
    #                    Workers heartbeat while a handler runs, so this only has
    #                    to cover a worker dying, not your slowest job.
    # max_attempts     - how many times a message may be handled before it is
    #                    dead-lettered; nil retries forever.
    # max_defers       - how many times a message may be deferred before it is
    #                    dead-lettered; nil (default) defers forever.
    # retry_backoff    - delay before retrying a failed message.
    # defer_backoff    - delay before a deferred message comes back.
    # delete_on_ack    - also XDEL entries once acked, so the stream doesn't
    #                    grow forever. Only safe with a single consumer group
    #                    (which is what Queekiq uses).
    # redis            - a client for this queue; defaults to Queekiq.redis.
    def initialize(name, stream: nil, group: nil, deferred_key: nil, dead_key: nil, field: DEFAULT_FIELD,
                   serializer: Serializers::Raw, reclaim_after_ms: DEFAULT_RECLAIM_AFTER_MS,
                   max_attempts: DEFAULT_MAX_ATTEMPTS, max_defers: nil,
                   retry_backoff: Backoff.new(base_ms: 1_000, max_ms: 300_000),
                   defer_backoff: Backoff.new(base_ms: 200, max_ms: 5_000),
                   delete_on_ack: false, redis: nil)
      raise ArgumentError, "queue name can't be blank" if name.to_s.strip.empty?
      raise ArgumentError, "field can't be one of #{RESERVED_FIELDS.join(", ")}" if RESERVED_FIELDS.include?(field.to_s)

      @name = name.to_s
      @stream = stream || "queekiq:{#{@name}}:stream"
      @group = group || "queekiq-#{@name}"
      @deferred_key = deferred_key || "queekiq:{#{@name}}:deferred"
      @dead_key = dead_key || "queekiq:{#{@name}}:dead"
      @field = field.to_s
      @serializer = serializer
      @reclaim_after_ms = reclaim_after_ms
      @max_attempts = max_attempts
      @max_defers = max_defers
      @retry_backoff = retry_backoff
      @defer_backoff = defer_backoff
      @delete_on_ack = delete_on_ack
      @redis = redis
    end

    def redis
      @redis || Queekiq.redis
    end

    # --- producing -----------------------------------------------------------

    # Appends a message to the stream and returns its entry id. The payload goes
    # through the serializer (the default one stores `payload.to_s`).
    def enqueue(payload)
      redis.xadd(stream, { field => serializer.dump(payload), "enqueued_at" => now_ms })
    end

    # --- moving messages around ----------------------------------------------

    # Schedules a message to come back later. Given a delivered Message, the
    # delivery is acked in the same atomic step, so the message is never both
    # pending and deferred. Given a bare payload it just schedules a new one.
    # The delay comes from defer_backoff (by how often it was deferred before)
    # unless `delay_ms` is given. Returns the delay used.
    def defer(message_or_payload, delay_ms: nil)
      message = as_message(message_or_payload)
      defers = message.defers + 1
      delay = delay_ms || defer_backoff.delay_ms(defers)
      schedule(message, delay, attempt: message.attempt, defers: defers)
      delay
    end

    # Schedules a failed message for another attempt (attempt + 1) and acks the
    # current delivery, atomically. The delay comes from retry_backoff unless
    # `delay_ms` is given. Returns the delay used.
    def retry_later(message, delay_ms: nil)
      delay = delay_ms || retry_backoff.delay_ms(message.attempt)
      schedule(message, delay, attempt: message.attempt + 1, defers: message.defers)
      delay
    end

    # Moves a message to the dead stream, with the reason and the error that
    # caused it, and acks the delivery, atomically.
    def dead_letter(message, reason:, error: nil)
      entry = { field => message.raw, "reason" => reason.to_s, "attempt" => message.attempt,
                "defers" => message.defers, "enqueued_at" => message.enqueued_at,
                "dead_at" => now_ms, "original_id" => message.id }
      if error
        entry["error_class"] = error.class.name
        entry["error_message"] = error.message.to_s[0, 1_000]
      end
      redis.multi do |tx|
        tx.xadd(dead_key, entry.compact)
        ack_commands(tx, message.id) if message.id
      end
    end

    # True once a message has used up its attempts (attempt > max_attempts), e.g.
    # because it keeps crashing its workers. Workers dead-letter these without
    # running the handler again.
    def attempts_exhausted?(message)
      !max_attempts.nil? && message.attempt > max_attempts
    end

    # True if the message is on its last allowed attempt, so a failure now is final.
    def last_attempt?(message)
      !max_attempts.nil? && message.attempt >= max_attempts
    end

    # True if the message has already been deferred max_defers times.
    def defers_exhausted?(message)
      !max_defers.nil? && message.defers >= max_defers
    end

    # Re-enqueues up to `limit` deferred messages whose delay has elapsed.
    # Returns how many were promoted.
    def promote_due!(limit: 50)
      redis.eval(PROMOTE_SCRIPT, keys: [ deferred_key, stream ], argv: [ now_ms, limit, field ])
    end

    # --- consuming -----------------------------------------------------------

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
      (result&.dig(stream) || []).map { |id, fields| build_message(id, fields) }
    end

    # Takes over pending messages that have been idle for reclaim_after_ms
    # (their consumer probably died) and returns them as Messages. Their
    # `attempt` includes the deliveries that never finished.
    def reclaim(consumer:, count: 10)
      result = redis.xautoclaim(stream, group, consumer, reclaim_after_ms, "0-0", count: count)
      entries = result.is_a?(Hash) ? result["entries"] : result[1]
      (entries || []).map { |id, fields| build_message(id, fields, deliveries: delivery_count(id)) }
    end

    # Heartbeat: resets the idle timer of a message this consumer is still
    # working on, so it isn't reclaimed. Returns false if the message no longer
    # belongs to this consumer (it was acked or reclaimed meanwhile).
    def touch(message_or_id, consumer:)
      id = message_or_id.respond_to?(:id) ? message_or_id.id : message_or_id
      redis.eval(TOUCH_SCRIPT, keys: [ stream ], argv: [ group, consumer, id ]) == 1
    end

    # Marks a message as done. Accepts a Message or a bare stream entry id.
    def ack(message_or_id)
      id = message_or_id.respond_to?(:id) ? message_or_id.id : message_or_id
      redis.multi { |tx| ack_commands(tx, id) }
    end

    # --- inspecting ----------------------------------------------------------

    # Number of entries currently in the stream (including acked ones that
    # haven't been deleted).
    def size
      redis.xlen(stream)
    end

    # Number of messages waiting in the deferred set.
    def deferred_size
      redis.zcard(deferred_key)
    end

    # Number of messages in the dead stream.
    def dead_size
      redis.xlen(dead_key)
    end

    # A snapshot for dashboards and alerts:
    #   backlog                - entries not yet delivered to the group (nil if Redis can't tell)
    #   pending                - delivered but not acked
    #   oldest_pending_idle_ms - how long the oldest pending message has been
    #                            idle; large values mean stuck or dead workers
    #   deferred / dead        - sizes of those sets
    #   consumers              - [{ name:, pending:, idle_ms: }] per consumer
    def stats
      info = group_info
      {
        backlog: info && info["lag"],
        pending: info ? info["pending"] : 0,
        oldest_pending_idle_ms: info && info["pending"].positive? ? oldest_pending_idle_ms : nil,
        deferred: deferred_size,
        dead: dead_size,
        consumers: info ? consumers : []
      }
    end

    # The oldest `count` dead messages, as DeadMessage structs.
    def dead_messages(count: 100)
      redis.xrange(dead_key, "-", "+", count: count).map { |id, fields| build_dead_message(id, fields) }
    end

    # Puts a dead message back on the stream with a fresh attempt counter and
    # removes it from the dead stream. Returns false if there's no such entry.
    def requeue_dead(id)
      entry = redis.xrange(dead_key, id, id).first
      return false unless entry

      raw = entry.last.fetch(field)
      redis.multi do |tx|
        tx.xadd(stream, { field => raw, "enqueued_at" => now_ms })
        tx.xdel(dead_key, id)
      end
      true
    end

    # Removes a message from the dead stream for good.
    def delete_dead(id)
      redis.xdel(dead_key, id).positive?
    end

    # Deletes the stream (and its group), the deferred set and the dead stream.
    # Meant for tests.
    def clear!
      redis.del(stream, deferred_key, dead_key)
    end

    private

    def now_ms
      (Time.now.to_f * 1000).to_i
    end

    def as_message(message_or_payload)
      return message_or_payload if message_or_payload.is_a?(Message)

      Message.new(id: nil, payload: message_or_payload, serializer: serializer, enqueued_at: now_ms)
    end

    # Writes the message into the deferred set and acks its delivery, atomically.
    def schedule(message, delay_ms, attempt:, defers:)
      member = [ attempt, defers, message.enqueued_at || now_ms, message.raw ].join("|")
      redis.multi do |tx|
        tx.zadd(deferred_key, now_ms + delay_ms, member)
        ack_commands(tx, message.id) if message.id
      end
    end

    def ack_commands(tx, id)
      tx.xack(stream, group, id)
      tx.xdel(stream, id) if @delete_on_ack
    end

    def build_message(id, fields, deliveries: 1)
      raw = fields.fetch(field) { raise Error, "stream entry #{id} has no #{field.inspect} field" }
      carried = fields["attempt"] ? fields["attempt"].to_i : 1
      Message.new(id: id, raw: raw, serializer: serializer,
                  attempt: carried + deliveries - 1,
                  defers: fields["defers"].to_i,
                  enqueued_at: fields["enqueued_at"]&.to_i)
    end

    def build_dead_message(id, fields)
      DeadMessage.new(id: id, raw: fields[field], reason: fields["reason"],
                      error_class: fields["error_class"], error_message: fields["error_message"],
                      attempt: fields["attempt"]&.to_i, defers: fields["defers"]&.to_i,
                      enqueued_at: fields["enqueued_at"]&.to_i, dead_at: fields["dead_at"]&.to_i,
                      original_id: fields["original_id"])
    end

    # How many times Redis has delivered this entry (XAUTOCLAIM counts each claim).
    def delivery_count(id)
      pending = redis.xpending(stream, group, id, id, 1).first
      pending ? pending["count"] : 1
    end

    def group_info
      redis.xinfo(:groups, stream).find { |g| g["name"] == group }
    rescue Redis::CommandError => e
      raise unless e.message.include?("no such key")

      nil
    end

    def consumers
      redis.xinfo(:consumers, stream, group).map { |c| { name: c["name"], pending: c["pending"], idle_ms: c["idle"] } }
    end

    def oldest_pending_idle_ms
      redis.xpending(stream, group, "-", "+", 1).first&.fetch("elapsed")
    end
  end
end
