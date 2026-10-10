# Queekiq

A small, framework-agnostic work queue on **Redis Streams**. No dependencies
beyond the `redis` gem.

- **Parallel workers** through consumer groups: every message goes to exactly one worker.
- **At-least-once delivery**: messages stay pending until acked, and messages
  abandoned by a crashed worker are reclaimed by another.
- **Retries with backoff and a dead-letter stream** for messages that keep failing.
- **Deferral**: a handler can say "not yet" and the message comes back shortly,
  which is enough to build per-key ordering without locks.
- **A worker loop that survives**: heartbeats for long jobs, graceful shutdown,
  reconnects when Redis goes away.
- **Observable**: queue stats, instrumentation events, structured logs.

Works with or without Rails (it uses `Rails.logger` automatically if present).
Requires Ruby >= 3.1, Redis >= 7.0 and the `redis` gem (4.8 up to 6.x; redis 6.x itself needs Ruby >= 3.2).

## Installation

```ruby
gem "queekiq", github: "your-user/queekiq"   # or path: "../queekiq" while developing
```

## Quick start

```ruby
# config/initializers/queekiq.rb (optional; defaults to ENV["REDIS_URL"])
Queekiq.configure do |c|
  c.redis_url = ENV.fetch("REDIS_URL", "redis://localhost:6379/0")
end

EMAILS = Queekiq::Queue.new("emails", serializer: Queekiq::Serializers::JSON)

# Producer (web process, console, ...)
EMAILS.enqueue("user_id" => user.id, "template" => "welcome")

# Consumer (bin/email_worker)
worker = Queekiq::Worker.new(queue: EMAILS) do |message|
  Mailer.deliver(**message.payload.transform_keys(&:to_sym))
  :sent                          # any return value is the "outcome"
end
worker.start                     # traps INT/TERM, runs until stopped
```

Run as many worker processes as you like. Consumer names default to
`hostname-pid`, which is unique per process, including under Docker.

## How it works

| Piece | Redis structure | Default key |
|---|---|---|
| Messages | stream | `queekiq:{name}:stream` |
| Workers | consumer group | `queekiq-<name>` |
| Deferred and retrying messages | sorted set (score = due time in ms) | `queekiq:{name}:deferred` |
| Messages that gave up | stream | `queekiq:{name}:dead` |

Each worker iteration:

1. **Promote** deferred messages that are due back onto the stream (atomically,
   in a Lua script, so none are lost or promoted twice).
2. **Reclaim** pending messages idle for longer than `reclaim_after_ms`
   (default 30 s), i.e. ones whose worker died.
3. **Read** new messages, blocking up to `block_ms` (default 1 s).

Each message goes through your handler and is then settled:

| Handler | What happens |
|---|---|
| returns normally | the message is acked |
| returns an outcome in `defer_on` (default `:deferred`) | it comes back after a delay (`defer_backoff`); dead-lettered after `max_defers` |
| raises | it is retried after a delay (`retry_backoff`); dead-lettered after `max_attempts` |
| the worker dies mid-handler | another worker reclaims it after `reclaim_after_ms` |

### Delivery semantics

Delivery is **at-least-once**. A message can be handled more than once (a worker
dies after the handler finished but before the ack), so handlers must be
idempotent. Ordering is never guaranteed by the queue itself; see Deferral.

## Messages and serializers

The handler receives a `Queekiq::Message`:

| | |
|---|---|
| `payload` | what you enqueued, decoded by the serializer (lazily) |
| `raw` | the payload as stored, a String |
| `id` | the stream entry id |
| `attempt` | 1 on the first delivery; +1 per retry or per delivery that never finished |
| `defers` | how many times it has been deferred |
| `enqueued_at` | when it was first enqueued (epoch ms); `latency_ms` is the age |

Serializers are anything with `dump(object) -> String` and `load(String) -> object`:
`Serializers::Raw` (default, plain strings, like Redis itself), `Serializers::JSON`
(hash keys come back as strings), or your own (MessagePack, Marshal, ...).
Because decoding is lazy, a payload that can't be decoded raises inside your
handler, and goes through retries and the dead stream like any other failure.

## Deferral and ordering

A handler returning `:deferred` means "not yet, try again shortly". The message
keeps its history, and each deferral waits a little longer (200 ms, 400 ms, ...
up to 5 s by default).

That is the building block for ordering. For example, process jobs for the same
account strictly in submission order, with the rule living in your database:

```ruby
Queekiq::Worker.new(queue: queue) do |message|
  job = Job.find(message.payload)
  next :already_done unless job.pending?
  next :deferred if Job.earlier_pending_for_same_account?(job)

  job.run!
  :done
end
```

The ordering key can be anything your database can compare: a sequence number,
a ULID, a timestamp, or a state machine ("a refund can only apply to a captured
payment"). Use `max_defers` if a message that waits forever should end up in the
dead stream instead.

## Failures, retries and dead letters

A raising handler is logged, passed to `on_error` (e.g. to report to an error
tracker), and retried after `retry_backoff` (default 1 s, doubling, up to 5 min,
with jitter). After `max_attempts` (default 5) it is moved to the dead stream
with the reason and the error. A message whose workers keep *dying* is
dead-lettered the same way, without running the handler again.

```ruby
queue.dead_size                              # => 1
dead = queue.dead_messages.first             # DeadMessage
dead.raw; dead.reason; dead.error_class; dead.error_message; dead.attempt
queue.requeue_dead(dead.id)                  # back on the queue, attempt reset
queue.delete_dead(dead.id)
```

Set `max_attempts: nil` to retry forever. A handler that should never be
retried can rescue its own errors and return an outcome instead.

## Long jobs

While a handler runs, the worker touches its message every `reclaim_after_ms / 3`
(`heartbeat_interval_ms`; `false` disables it). A slow job is therefore never
reclaimed and run twice just for being slow, while a worker that really died
stops heartbeating and is reclaimed after `reclaim_after_ms`. A worker that lost
its message (it was reclaimed anyway) can't take it back.

## Shutdown

`worker.stop` (called for you on INT/TERM by `start`) lets the message in hand
finish. If that takes longer than `shutdown_timeout` (default 25 s), the worker
gives up on it and exits; the message stays pending and another worker reclaims
it. Set `shutdown_timeout:` below your platform's kill grace period (Docker's is
10 s by default, Kubernetes' 30 s).

## When Redis goes away

Connection errors don't crash the worker: it waits (`reconnect_backoff`, 0.5 s
doubling up to 30 s) and tries again. A stream or consumer group that vanished
(Redis restarted without persistence) is recreated. Messages being handled stay
pending and are reclaimed afterwards.

## Observability

```ruby
queue.stats
# => { backlog: 12, pending: 2, oldest_pending_idle_ms: 340, deferred: 3, dead: 0,
#      consumers: [{ name: "web1-123", pending: 1, idle_ms: 5 }, ...] }
```

`oldest_pending_idle_ms` is the one to alert on: a large value means workers are
stuck or dead. `backlog` is nil if Redis can't tell (after entries were deleted).

Workers emit events with these names, each with `queue:` and `consumer:` and a
`duration_ms:`:

| Event | Extra payload |
|---|---|
| `process.queekiq` | `message`, `outcome`, `latency_ms`, `exception` if the handler raised |
| `defer.queekiq` | `message`, `delay_ms` |
| `retry.queekiq` | `message`, `delay_ms`, `error` |
| `dead_letter.queekiq` | `message`, `reason`, `error` |
| `reclaim.queekiq` | `count` |
| `connection_error.queekiq` | `error`, `retry_in_ms`, `failures` |

```ruby
Queekiq.subscribe("process.queekiq") do |_name, payload|
  StatsD.timing("queekiq.process", payload[:duration_ms], tags: [ "queue:#{payload[:queue]}" ])
  StatsD.timing("queekiq.latency", payload[:latency_ms]) if payload[:latency_ms]
end

# Or in Rails, use ActiveSupport::Notifications instead:
Queekiq.configure { |c| c.instrumenter = ActiveSupport::Notifications }
```

Logs are one `key=value` line per event, e.g.
`[queekiq] event=retry queue=emails consumer=web1-123 id=1-0 payload=42 attempt=1 delay_ms=1043`.
Payloads are truncated to 100 characters, but don't put secrets in them.

## Configuration

Global (`Queekiq.configure`): `redis_url`, `redis` (an existing client),
`logger` (defaults to `Rails.logger`, else stdout), `instrumenter`.

`Queekiq::Queue.new(name, **options)`:

| Option | Default | |
|---|---|---|
| `stream`, `group`, `deferred_key`, `dead_key` | derived from `name` | Override to adopt existing keys. |
| `field` | `"payload"` | Stream field holding the payload. |
| `serializer` | `Serializers::Raw` | How payloads are stored. |
| `reclaim_after_ms` | `30_000` | Idle time before a pending message is reclaimed. |
| `max_attempts` | `5` | Failed attempts before dead-lettering; `nil` = forever. |
| `max_defers` | `nil` | Deferrals before dead-lettering; `nil` = forever. |
| `retry_backoff` | `Backoff.new(base_ms: 1_000, max_ms: 300_000)` | Delay after a failure. |
| `defer_backoff` | `Backoff.new(base_ms: 200, max_ms: 5_000)` | Delay after a deferral. |
| `delete_on_ack` | `false` | `XDEL` entries once acked so the stream doesn't grow without bound. |
| `redis` | `Queekiq.redis` | A client dedicated to this queue. |

`Queekiq::Worker.new(queue:, handler: nil, **options, &block)`:

| Option | Default | |
|---|---|---|
| `consumer` | `hostname-pid` | Unique per worker process. |
| `defer_on` | `[:deferred]` | Outcomes that defer the message. |
| `block_ms` | `1_000` | How long one read blocks. |
| `on_error` | `nil` | `#call(error, message)`. |
| `heartbeat_interval_ms` | `reclaim_after_ms / 3` | `false` disables. |
| `shutdown_timeout` | `25` (seconds) | `nil` waits forever. |
| `reconnect_backoff` | `Backoff.new(base_ms: 500, max_ms: 30_000)` | Pause while Redis is down. |
| `logger` | `Queekiq.logger` | |

`Queekiq::Backoff.new(base_ms:, max_ms:, factor: 2, jitter: 0.2)` is exponential
with jitter; `Backoff.fixed(ms)` is constant; anything with `delay_ms(attempt)` works.

Use one Redis connection per worker thread: a blocking read occupies its
connection. On Redis Cluster, the stream, deferred and dead keys must share a
hash tag (the defaults do).

## Low-level API

`Queekiq::Queue` can be used without `Worker`: `enqueue`, `defer`, `retry_later`,
`dead_letter`, `promote_due!`, `ensure_group!`, `read`, `reclaim`, `touch`, `ack`,
plus `stats`, `size`, `deferred_size`, `dead_size` and `clear!` (for tests).

## Development

```bash
docker compose up -d redis   # or any Redis >= 7 on REDIS_URL
bundle install
bundle exec rake             # specs
bundle exec rubocop
```

CI (GitHub Actions) runs the lint, the specs on Ruby 3.1-3.4, the redis gem 4.8,
5.x and 6.x and Redis 7.0 and 7.4, and builds and installs the gem.
To run the specs against another redis gem locally: `REDIS_GEM_VERSION="~> 5.0" bundle update redis`.

## Not included (yet)

Per-key rate limits or leases, sharded streams for strict per-key ordering,
scheduling brand-new work for later (only deferral and retries), replay and
multiple consumer groups, a web UI, and an in-memory backend for tests.

## License

MIT.
