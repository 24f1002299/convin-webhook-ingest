# Solution

## What was broken

Webhook ingestion had several independent failure modes. Event deduplication was a check-then-write operation without a durable uniqueness constraint, so concurrent or repeated deliveries could both apply their effects. The event, call, and aggregate writes were separate operations, allowing partial state and inflated totals. The statistics cache also mutated shared entries without exclusive synchronization, which caused a data race and lost increments.

Recording work ran in request-scoped goroutines. A request or deployment could cancel that work, failures were neither persisted nor retried, and a restart had no durable queue to recover. The cache started empty after every restart, and shutdown could close Postgres while workers were still using it.

The fixes make `events.event_id` unique after removing historical duplicates and rebuilding account totals. `IngestEvent` now uses one Postgres transaction: it inserts the event with `ON CONFLICT DO NOTHING` and, only for a new event, upserts the call, increments `account_stats`, and enqueues one recording job. Any error rolls back all four effects. A duplicate commits no effects and is returned to the provider as a successful no-op. The in-memory cache is changed only after a confirmed commit, is mutex-protected, and is hydrated from durable totals before traffic is accepted.

Recording jobs now live in Postgres with attempts, retry time, error, and lease state. Workers claim with `FOR UPDATE SKIP LOCKED`, use bounded exponential backoff, and can reclaim expired leases after a crash. Completing a job atomically marks the call processed and deletes the job. Shutdown stops HTTP traffic, cancels and joins workers, and only then closes Postgres.

## Why Postgres, not Redis

Postgres already owns every durable consequence of accepting a webhook. Its unique constraint is the final arbiter under concurrent delivery, and one transaction can couple deduplication with the call, aggregate, and job writes. A Redis deduplication key would introduce a second consistency domain: a crash between Redis and Postgres could either lose an event or apply it twice, and key expiry, eviction, or data loss would weaken the guarantee. Solving that correctly would require additional coordination without improving correctness here. Redis can still be used later as a disposable read cache, but it is not in the ingestion correctness path.

The exactly-once guarantee applies to committed database effects for a stable `event_id`; delivery and recording processing remain at least once. The cache is deliberately not part of the database transaction. A crash after commit but before `Record` can temporarily leave that process's cache behind, but Postgres remains correct and startup hydration repairs it.

## Path to 10,000 webhooks per second

I would load-test with production account skew first, then horizontally scale stateless HTTP instances behind a load balancer and use a right-sized pool or PgBouncer. The current per-event transaction is correctness-first; at sustained 10,000/s I would batch ingestion through a partitioned durable log, bulk-insert events, and apply effects with idempotent consumers. Events and jobs would be partitioned, retention-managed, and indexed from measured query plans. Hot `account_stats` rows would need sharded counters or an append-only aggregation stream to avoid serializing all traffic for a large account.

Recording workers would claim batches and process with bounded concurrency, rate limits, backpressure, and queue-depth/lease/retry metrics. Read traffic could use Redis or replicas as rebuildable caches. I would preserve the Postgres unique key (or an equivalently durable idempotency ledger), because scaling throughput must not move deduplication back to a best-effort cache.
