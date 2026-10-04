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