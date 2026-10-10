# Changelog

## [Unreleased]

## [0.2.0]

### Added

- **Message envelope.** Every message carries `attempt`, `defers` and `enqueued_at`
  (readable on `Queekiq::Message`), which survive deferrals and retries.
- **Serializers.** `serializer:` on `Queue` (`Serializers::Raw`, the default, or
  `Serializers::JSON`, or your own `dump`/`load` object). Payloads are decoded
  lazily, so an undecodable payload fails inside the handler and is retried and
  dead-lettered instead of crashing the worker.
- **Retries with backoff.** A handler that raises is retried after `retry_backoff`
  (exponential with jitter). New `Queekiq::Backoff`.
- **Dead-letter stream.** After `max_attempts` (default 5) failed attempts, or
  `max_defers` deferrals (default: unlimited), a message moves to the queue's
  dead stream with the reason and error. `Queue#dead_messages`, `#requeue_dead`,
  `#delete_dead`, `#dead_size`. Messages whose workers keep dying are dead-lettered
  too, without running the handler again.
- **Deferral backoff.** `defer_backoff` grows the delay each time a message is
  deferred (default 200 ms up to 5 s).
- **Heartbeat.** While a handler runs, the worker keeps the message from being
  reclaimed (`Queue#touch`, `heartbeat_interval_ms`), so `reclaim_after_ms` only
  has to cover a worker dying, not your slowest job. A worker that lost its
  message to a reclaim can't steal it back.
- **Resilient worker loop.** Lost Redis connections are retried with backoff
  (`reconnect_backoff`); a stream or group that disappeared (Redis restarted
  empty) is recreated.
- **Safer shutdown.** `stop` finishes the message in hand and, if that takes longer
  than `shutdown_timeout` (default 25 s), abandons it so another worker reclaims it.
- **Observability.** `Queue#stats` (backlog, pending, oldest pending idle time,
  deferred, dead, per-consumer pending) and instrumentation events
  (`process`, `defer`, `retry`, `dead_letter`, `reclaim`, `connection_error`,
  all `.queekiq`) via `Queekiq.subscribe`, or ActiveSupport::Notifications through
  `config.instrumenter`. Log lines are now structured `key=value`.
- GitHub Actions CI: lint, specs on Ruby 3.1-3.4 / redis gem 4.8-6.x / Redis 7.0 and
  7.4, and a build-and-install check; Dependabot for actions and gems.

### Changed

- `Queue#defer(message)` now takes the delivered message and acks it in the same
  atomic step; a bare payload still works for scheduling something new. The
  worker no longer calls `ack` after a deferral.
- Failed messages are retried with backoff instead of waiting out
  `reclaim_after_ms`, and are dead-lettered after `max_attempts`. Pass
  `max_attempts: nil` to retry forever.
- Deferral delays now grow (200 ms, 400 ms, ... up to 5 s). Use
  `defer_backoff: Queekiq::Backoff.fixed(200)` for the old fixed delay.
- `Queue.new(defer_delay_ms:)` is replaced by `defer_backoff:`.
- `Message` is a class, not a Struct.
- `clear!` also deletes the dead stream.

### Upgrading from 0.1

- Deferred entries are now stored as `attempt|defers|enqueued_at|payload`. A 0.1
  worker would promote one of those with the whole string as its payload, so
  stop all 0.1 workers before starting 0.2 ones. 0.2 reads everything 0.1 wrote.
- Specs that stub `Queue#defer` and expect the payload need to expect the
  message: `have_received(:defer).with(having_attributes(payload: "..."))`.

## [0.1.0]

- Initial extraction: `Queekiq::Queue` (Redis Streams + consumer group, reclaim,
  deferral), `Queekiq::Worker` (consumer loop), `Queekiq::Configuration`.
