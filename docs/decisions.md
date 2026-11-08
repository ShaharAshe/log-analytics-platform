# Architecture Decisions

## ADR-001: Use Kafka instead of a message queue

**Context:** Applications send logs to a central platform. We need durability
under crashes, buffering during spikes, multiple independent consumers, and the
ability to reprocess historical data after fixing a bug.

**Decision:** Use Kafka (an append-only log) rather than a traditional message
queue such as RabbitMQ.

**Rationale:** In a queue, reading removes the message and the broker owns the
consumption state. In a log, messages are retained and each consumer group owns
its offset. That single difference gives us replay, independent consumers, and
spike absorption at once.

**Trade-off:** More operational complexity than a queue. No per-message
acknowledgement or redelivery semantics — we manage offsets ourselves.

---

## ADR-002: Partition key is `service`

**Context:** The consumer maintains per-service rolling counters (error rates,
latencies) in memory. Those counters are only correct if all logs for a given
service are handled by the same consumer instance.

**Decision:** Use the `service` field as the Kafka partition key.

**Rationale:** Kafka routes by `hash(key) % num_partitions`, so the same key
always lands on the same partition, and a partition is assigned to exactly one
consumer in a group. This gives us locality: one service's counter lives in one
place and needs no cross-instance coordination. Ordering within a service is
preserved as a side effect.

**Rejected alternatives:**
- `level` — only 4-5 distinct values (low cardinality). Most partitions would
  stay empty regardless of partition count.
- `trace_id` — near-unique per request, so a service would spread across all
  partitions and locality would be lost.

**Trade-off:** A service producing a large share of traffic creates a hot
partition. Mitigation (not implemented yet): key salting — suffix noisy services
with `-0`, `-1`, `-2` and merge the counters at aggregation time.

---

## ADR-003: 12 partitions for `raw-logs`

**Context:** Partition count caps how many consumers in a group can work in
parallel. Adding partitions later changes `hash(key) % num_partitions`, which
reassigns existing keys to different partitions.

**Decision:** Create `raw-logs` with 12 partitions, well above current need.

**Rationale:** Over-provisioning is cheap — a partition costs some metadata and
open file handles. Adding partitions later is expensive: in-memory counters on
the old partition are stranded while new logs for the same service arrive at a
different consumer, so the service gets counted twice and both counts are wrong.
Ordering breaks too, since old and new records for a key sit in different
partitions. 12 leaves room to scale consumers from 3 to 12 without touching the
topic.

**Trade-off:** Running fewer consumers than partitions means each consumer holds
several partitions. That is fine, but it does mean partition count alone does not
tell us actual parallelism.

---

## ADR-004: Return 202 Accepted even when Kafka is unavailable

**Context:** Other teams' services (e.g. `payment-api`) send logs to the ingest
service over HTTP. When Kafka is unreachable, `KafkaTemplate.send()` fails during
the metadata step after `max.block.ms` (3 s) and throws `KafkaException` on the
request thread. Without handling, Spring returns 500.

**Decision:** Treat sending as best effort. `LogProducer` catches
`KafkaException` and logs it at ERROR with the service, trace ID and full
exception. The endpoint always returns 202 Accepted.

**Rationale:** Callers monitor the status codes they receive and alert on 5xx.
Returning 503 would page the on-call engineers of every sending team for an
outage in our system, which they cannot fix and have no useful action for. The
outage still needs to be noticed, but by us: the alert belongs in our own
monitoring, not in theirs.

**Alternatives considered:**
- 503 Service Unavailable — honest and retry-friendly, but pushes our outage onto
  every sender's on-call and requires each sender to buffer and retry.
- 500 (the default) — misreports a dependency outage as a bug in our code.

**Trade-off:**
- Logs sent while Kafka is down are lost, and senders get no signal. Until we add
  a dropped-logs metric with an alert, the only trace is the ERROR log line.
- 202 does not remove the wait: each request still blocks for up to
  `max.block.ms` before returning (measured ~4.5 s with Kafka stopped). At high
  volume this ties up sender threads. Lowering `max.block.ms` shortens the wait
  but drops logs during brief Kafka hiccups.

---

## ADR-005: Keep failed messages in a dead letter topic

**Context**

Malformed messages were logged and dropped (`ErrorHandlingDeserializer`, see the
poison pill fix). The consumer no longer got stuck, but the message itself was lost:
there was no way to see what was sent, fix it, or send it again.

**Decision**

- Messages that fail are published to a dead letter topic, `raw-logs-dlt`, and the
  consumer moves on.
- The processor creates `raw-logs-dlt` on startup, with the same number of partitions
  as `raw-logs` (12).
- Deserialization errors go to the DLT immediately, without retries.
- If the listener itself fails, the message is retried twice, 1 second apart
  (3 attempts in total), before it goes to the DLT.
- The message value is published as its original raw bytes (`ByteArraySerializer`).
  The error details and the original topic, partition and offset are added as headers.

**Rationale**

- A DLT means a failed message is neither lost nor allowed to block the partition.
- The recoverer publishes to the same partition number the message came from, so a
  message from partition 7 is found in partition 7 of the DLT.
- A failed message may not be valid JSON. A JSON serializer would not fail, but it
  would store a base64 string instead of the original message, so replaying it would
  send something different from what was sent.
- Retrying cannot fix broken data, so deserialization errors are not retried.
- Retries are few and short because the partition is blocked while they wait. A more
  detailed retry policy will be chosen when the listener has a dependency that can
  actually fail (Redis) and can be tested.

**Alternatives considered**

- *Keep logging and dropping failed messages.* Simple, but the message is lost and
  cannot be inspected or replayed.
- *A DLT with a single partition.* It works: when the target partition does not
  exist, the recoverer lets Kafka choose one. Rejected because the partition mapping
  is lost and a warning is logged for every failed message.

**Trade-offs**

- *A failed dependency is not a bad message.* If Redis is down, every message fails,
  including valid ones. After the retries, all of them go to the DLT, and the
  pipeline appears to work while processing nothing. Retries and a DLT fit bad
  messages, not outages. Pausing the consumer until the dependency recovers will be
  handled together with Redis.
- *Only raw bytes can be published.* When deserialization succeeds and the listener
  fails, the recoverer will try to publish a `LogEvent` object, which
  `ByteArraySerializer` cannot handle, and publishing to the DLT will fail. This
  needs a second template for `LogEvent` values once the listener can fail.
- *Retries block the partition.* While a message waits between retries, the messages
  behind it in the same partition wait too (up to about 2 seconds per message).

> Trade-offs 1 and 2 are addressed by ADR-006.

---

## ADR-006: Handle failures by type: retry infrastructure failures, send bad messages to the DLT

**Context**

With ADR-005 in place, the listener started using Redis. Stopping Redis on purpose
showed two problems:

- Every `ERROR` log failed, valid ones included. After the retries, publishing to the
  DLT also failed (`ByteArraySerializer` cannot handle a `LogEvent`), so the consumer
  looped. No message was lost, but only by accident: had the DLT worked, every valid
  message would have been moved to it while consumer lag stayed at 0.
- When an open Redis connection drops, Lettuce does not fail. It queues the command,
  reconnects in the background, and fails only after its default command timeout of
  60 seconds, with a timeout exception rather than a connection error.

In addition, logs without a `service` were counted under the key `error:null`, which
silently produced wrong data.

**Decision**

- Failures are classified by type, walking the exception's cause chain, because Spring
  wraps listener exceptions in `ListenerExecutionFailedException`.
- *Bad messages* (`DeserializationException`, `InvalidLogEventException`) are not
  retried and go to the DLT immediately.
- *Infrastructure failures* (`RedisConnectionFailureException`, `QueryTimeoutException`)
  are retried every 5 seconds with no limit, and never go to the DLT.
- *Any other exception* is retried twice, 1 second apart, then goes to the DLT.
- The Redis command and connect timeouts are set to 2 seconds.
- The listener throws `InvalidLogEventException` for a log without a `service`.
- The DLT recoverer uses two templates: raw bytes for messages that failed
  deserialization, and JSON for `LogEvent` values the listener rejected. The JSON
  template is created inside the configuration, not as a bean, so it does not replace
  the auto-configured template.

**Rationale**

- An infrastructure failure affects every message, not one. The messages are valid and
  wait safely in Kafka. Growing lag makes the outage visible, and processing resumes on
  its own when Redis returns.
- A bad message fails the same way on every attempt, so retrying only delays the
  messages behind it.
- Unexpected exceptions keep a small retry budget, so a bug cannot block the consumer
  forever.
- A 2-second timeout detects an outage quickly, and the timeout exception is
  classified as an infrastructure failure, so it never reaches the DLT.
- A custom exception is classified safely: a generic one such as
  `IllegalArgumentException` could come from any library.

**Alternatives considered**

- *Unlimited retries for every exception.* Rejected: a bug such as a
  `NullPointerException` would block the consumer forever, like a poison pill.
- *Keep the Lettuce default timeout of 60 seconds.* Rejected: an outage is detected
  only after a minute, and every attempt holds the partition for that long.
- *Count logs without a `service` under a placeholder key.* Rejected: it mixes
  unrelated logs into one counter and hides the real problem.

**Trade-offs**

- During a Redis outage the whole consumer waits, including logs that are not `ERROR`
  and would not need Redis.
- The DLT holds two formats: original bytes, or a JSON copy rebuilt from `LogEvent`.
  The JSON copy is not byte-identical to the original (missing fields appear as
  `null`).
- While retrying, the logs show only `Record in retry`, not the cause. A metric or a
  warning will be added with monitoring.
- The JSON template repeats producer settings from the YAML (bootstrap servers, acks).
- Any misconfiguration of a DLT template turns every failure into a loop, because a
  message whose DLT publish fails is never committed.

---

## ADR-007: Count errors in a one-minute sliding window, scored by Kafka record time

**Context**

The processor kept one counter per service (`error:<service>`) that only grows. A count
of 500 errors says nothing about when they happened: 500 over a week is normal, 500 in a
minute is an outage. Anomaly detection needs the number of errors per service in the
last minute.

**Decision**

- Each service has a Redis sorted set, `error-window:<service>`, with one member per
  error.
- The member is `partition:offset`, which is unique per Kafka message.
- The score is the Kafka record timestamp, set by ingest's producer when it sent the
  log (ingestion time).
- On every write, members older than one minute are removed (`ZREMRANGEBYSCORE`), and
  the key's TTL is reset to two minutes, so a service that stops failing loses its key.
- The four commands run separately, with no transaction and no Lua script.

**Rationale**

- A sliding window counts the last 60 seconds from any moment, so a burst that crosses
  a minute boundary is counted in full.
- A unique member makes the count idempotent: a message processed twice under
  at-least-once delivery updates its member instead of adding a new one.
- Ingestion time reflects when an error entered the system, not when it was processed.
  A backlog processed late, for example after a Redis outage, does not look like a
  spike.
- Each service's logs are in one partition (ADR-002), and a partition is read by one
  consumer at a time, so each key has a single writer. This holds for any number of
  instances or listener threads. Each command is also safe to repeat if the processor
  crashes midway, because the message is processed again from an uncommitted offset.

**Alternatives considered**

- *A fixed one-minute bucket per service* (`INCR` on a key per minute). Uses one number
  per minute, but a burst split across two buckets is never seen in full, and a
  redelivered message is counted twice.
- *Processing time as the score.* Turns a processed backlog into a false spike.
- *The `timestamp` field from the log JSON (event time).* Optional, so it can be
  missing, and set by the sender's clock, which the platform does not control.
- *A Lua script for the four commands.* Atomic and one network round trip, but adds a
  second language and is harder to debug, while a single writer per key makes
  atomicity unnecessary. Reconsider if load tests show the round trips matter.

**Trade-offs**

- Memory grows with the error rate: one member per error, not one number per minute.
  At 10,000 errors per minute per service this is still under a megabyte.
- Members are trimmed only on writes. A service that stops failing keeps its last
  errors until the key expires. Readers must count by score
  (`ZCOUNT key <now - 60s> +inf`), not by set size (`ZCARD`).
- Skipping the script depends on one writer per key. A key shared across services (such
  as a global error window) or a different partition key would break that assumption
  and require a script or a transaction.
- The window depends on the clocks of ingest and processor agreeing. A large clock skew
  would shift the window.