# Transport Alternatives

## Evaluation criteria

Choose a transport for durable delivery, keyed ordering, redelivery, backpressure, operations, and existing platform fit. Do not choose it because it can be made to look like a database or consensus system.

## Comparison

| Criterion | NATS JetStream | Kafka | Managed queue |
| --- | --- | --- | --- |
| Default fit for FlowRule | Strong | Strong when already standard | Strong when provider lock-in is acceptable |
| Ordering | Stream/consumer configuration; key routing must be designed | Native partition ordering | Group/session ordering varies by provider |
| Replay and retention | Good, configurable | Excellent, mature ecosystem | Usually limited or provider-specific |
| Operational burden | Lower | Higher | Lowest for managed service |
| Analytics ecosystem | Smaller | Excellent | Varies |
| Delivery model | Pull, ack, redelivery | Poll, commit, redelivery | Receive, visibility timeout, retry |
| Main risk | Misconfigured subjects/consumers | Treating partitions as a global scheduler | Weak replay/order semantics |

## Recommendation

Use NATS JetStream as the default adapter for a new deployment when low latency, durable work queues, and a small operating footprint matter. Use Kafka when the organization already operates Kafka, requires long retention, or needs its ecosystem. Use a managed queue when operational simplicity dominates portability.

The recommendation is an adapter decision, not a domain decision. Every adapter must satisfy the same contract:

- publish preserves event ID and partition key;
- fetch returns a delivery with ack and retry/dead-letter behavior;
- ack is sent only after the SQL transaction commits;
- redelivery is expected;
- broker metadata is not visible to rule evaluation;
- adapter conformance tests cover duplicates, redelivery, ordering, shutdown, and poison messages.

## Kafka-specific guidance

Kafka is not an alternative SQL transaction manager. Do not claim Kafka transactions make an HTTP destination exactly once. Use the Kafka key for the business partition key, commit offsets after SQL commit, and keep retry/quarantine policy explicit. If a retry topic changes the partition key or ordering, the application must record that semantic change.

## NATS-specific guidance

Use durable streams, pull consumers, explicit acknowledgement, bounded fetch batches, and a redelivery policy. Keep stream retention and consumer acknowledgement deadlines large enough for the SQL transaction and controlled shutdown. Do not use ephemeral consumers for business events.

## Migration and fallback

The broker port is intentionally small. A second adapter is added only after the first passes conformance tests and production-like failure tests. The engine must not contain `if kafka` or `if nats` branches; adapter-specific configuration belongs under deployment configuration.
