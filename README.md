# Queekiq

A small, framework-agnostic work queue on **Redis Streams**.

- Consumer groups, so any number of worker processes share one queue.
- At-least-once delivery: messages stay pending until acked, and messages
  abandoned by a crashed worker are reclaimed by another.
- **Deferral**: a handler can say "not yet" and the message comes back after a
  short delay, which is enough to build per-key ordering without locks.
- A ready-made worker loop with graceful shutdown.

No dependency on Rails (it uses `Rails.logger` automatically if present).
Requires Ruby >= 3.1, Redis >= 7.0 and the `redis` gem (4.8 up to 6.x).

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

EMAILS = Queekiq::Queue.new("emails")

# Producer (web process, console, ...)
EMAILS.enqueue(user.id)          # payloads are strings; use JSON for structured data

# Consumer (bin/email_worker)
worker = Queekiq::Worker.new(queue: EMAILS) do |message|
  Mailer.deliver(message.payload)
  :sent                          # any return value is the "outcome"
end
worker.start                     # traps INT/TERM, runs until stopped
```

Run as many worker processes as you like; each message goes to exactly one of
them. Consumer names default to `hostname-pid`, which is unique per process,
including under Docker.

## How it works

| Piece | Redis structure | Default key |
|---|---|---|
| Messages | stream | `queekiq:{name}:stream` |
| Workers | consumer group | `queekiq-<name>` |
| Deferred payloads | sorted set (score = due time in ms) | `queekiq:{name}:deferred` |

Each worker iteration:

1. **Promote** deferred payloads that are due back onto the stream (atomically,
   in a Lua script, so none are lost or promoted twice).
2. **Reclaim** pending messages idle for longer than `reclaim_after_ms`
   (default 30 s), i.e. ones whose worker died.
3. **Read** new messages, blocking up to `block_ms` (default 1 s).

Each message goes through your handler and is then acked.

### Delivery semantics

Delivery is **at-least-once**. A message can be handled more than once (a worker
dies after the handler finished but before the ack, or a handler is slower than
`reclaim_after_ms`), so handlers must be idempotent. Set `reclaim_after_ms`
comfortably above your slowest handler.

### Outcomes and deferral

The handler receives a `Queekiq::Message` (`#id`, `#payload`) and returns an
outcome. If the outcome is in `defer_on` (default `[:deferred]`) the payload is
scheduled to come back after the queue's `defer_delay_ms` (default 200 ms). The
original delivery is acked either way.

That is the building block for ordering. For example, process jobs for the same
account strictly in submission order, with the ordering rule living in your
database:

```ruby
Queekiq::Worker.new(queue: queue) do |message|
  job = Job.find(message.payload)
  next :already_done unless job.pending?
  next :deferred if Job.earlier_pending_for_same_account?(job)   # come back later

  job.run!
  :done
end
```

### Errors

If the handler raises, the worker logs it, calls `on_error` (if given, e.g. to
report to an error tracker) and leaves the message **unacked**, so it is
redelivered after `reclaim_after_ms`. A message that always raises will
therefore be retried forever; handlers that should give up should rescue their
own errors, record the failure and return an outcome instead.

```ruby
Queekiq::Worker.new(queue: queue, on_error: ->(error, message) { Sentry.capture_exception(error) }) { ... }
```

## Configuration

Global (`Queekiq.configure`): `redis_url`, `redis` (an existing client, e.g. a
shared one), `logger` (defaults to `Rails.logger`, else stdout).

`Queekiq::Queue.new(name, **options)`:

| Option | Default | |
|---|---|---|
| `stream`, `group`, `deferred_key` | derived from `name` | Override to adopt existing keys. |
| `field` | `"payload"` | Stream field holding the payload. |
| `reclaim_after_ms` | `30_000` | Idle time before a pending message is reclaimed. |
| `defer_delay_ms` | `200` | Default delay for `#defer`. |
| `delete_on_ack` | `false` | `XDEL` entries once acked so the stream doesn't grow without bound. |
| `redis` | `Queekiq.redis` | A client dedicated to this queue. |

`Queekiq::Worker.new(queue:, handler: nil, **options, &block)`:
`consumer`, `logger`, `defer_on` (`[:deferred]`), `block_ms` (`1_000`, also the
worst-case time to stop), `on_error`.

Use one Redis connection per worker thread: a blocking read occupies its
connection.

On Redis Cluster, `stream` and `deferred_key` must share a hash tag (the
defaults do).

## Low-level API

`Queekiq::Queue` can be used without `Worker`: `enqueue`, `defer`,
`promote_due!`, `ensure_group!`, `read`, `reclaim`, `ack`, plus `size`,
`deferred_size` and `clear!` (for tests).

## Development

```bash
docker compose up -d redis   # or any Redis >= 7 on REDIS_URL
bundle install
bundle exec rspec
```

## Not included (yet)

Delivery-count limits / dead-letter stream, stream trimming beyond
`delete_on_ack`, delayed enqueue of new work (only deferral of existing
payloads), metrics.

## License

MIT.
