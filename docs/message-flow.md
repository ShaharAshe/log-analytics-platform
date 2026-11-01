# Message flow

How a single log moves through the platform, including what happens when each step
fails. Solid lines are the normal path; dotted lines are failure paths.

```mermaid
flowchart TD
    C[Client service] -->|"POST /events/logs (JSON)"| I[ingest]
    I -->|"202 Accepted, before Kafka confirms"| C
    I -->|"key = service, partition = hash(service) % 12"| K[("Kafka: raw-logs<br/>12 partitions")]
    I -.->|"Kafka down: send() blocks up to 3 s"| KD["Error logged, log dropped,<br/>client still gets 202 (ADR-004)"]

    K -->|"poll, consumer group log-processor"| D{"Deserialize<br/>JSON to LogEvent"}
    D -.->|"not valid JSON"| DLT[("Kafka: raw-logs-dlt<br/>12 partitions")]
    D --> V{"service present?"}
    V -.->|"no: InvalidLogEventException"| DLT
    V --> L{"level is ERROR?"}
    L -->|no| CM[Commit offset]
    L -->|yes| R["INCR error:{service}"]
    R <--> RD[(Redis)]
    R -->|success| CM
    R -.->|"Redis down or command timeout (2 s)"| W["Retry every 5 s, no limit.<br/>Message waits in Kafka, lag grows."]
    W -.-> R
    R -.->|"unexpected exception, after 3 attempts"| DLT
    DLT --> CM
```

## Steps

| Step | What happens | If it fails |
| --- | --- | --- |
| Client to ingest | The client sends a log as JSON over HTTP. | Invalid request body: `400 Bad Request`. |
| ingest to Kafka | `KafkaTemplate.send()` with `service` as the key. The endpoint returns `202 Accepted` without waiting for Kafka. | Kafka unreachable: `send()` blocks for up to 3 s (`max.block.ms`), the error is logged, and the client still gets `202` (ADR-004). |
| Kafka to processor | The processor polls `raw-logs` as part of the `log-processor` consumer group. Each partition is read by one instance. | The processor is down: logs wait in Kafka and are read when it returns. |
| Deserialize | `ErrorHandlingDeserializer` turns the JSON into the processor's own `LogEvent`. | Not valid JSON: published to the DLT immediately, as the original bytes. |
| Validate | The listener checks that `service` is present. | Missing `service`: `InvalidLogEventException`, published to the DLT immediately, as JSON. |
| Count | For `ERROR` logs, `INCR error:{service}` in Redis. `INCR` is atomic, so concurrent instances never lose an update. | Redis down or a command times out after 2 s: retried every 5 s with no limit. Never sent to the DLT (ADR-006). Any other exception: retried twice, 1 s apart, then published to the DLT. |
| Commit | The offset is committed after processing, so delivery is at-least-once. | A crash before the commit: the message is processed again. |

## Why failures take different paths

A **bad message** fails the same way on every attempt, so retrying only delays the
messages behind it. It goes to the DLT, and the consumer moves on.

An **infrastructure failure** affects every message, valid ones included. Sending them
to the DLT would empty the topic silently while consumer lag stays at 0. Instead, the
messages wait in Kafka, growing lag makes the outage visible, and processing resumes on
its own when Redis returns.

An **unexpected exception** gets a small retry budget, so a bug cannot block the
consumer forever.

## What the DLT keeps

Each DLT message carries headers that explain the failure without searching the logs:

| Header | Meaning |
| --- | --- |
| `kafka_dlt-original-topic`, `-partition`, `-offset` | Where the message came from |
| `kafka_dlt-original-consumer-group` | The group that failed to process it |
| `kafka_dlt-exception-fqcn`, `-cause-fqcn` | The exception and its cause |
| `kafka_dlt-exception-message`, `-stacktrace` | The error message and stack trace |

The value is the original bytes when deserialization failed, or a JSON copy of the
`LogEvent` when the listener rejected the message.

See [`decisions.md`](decisions.md) for the reasoning behind each choice (ADR-004 to
ADR-006).
