# Contract: Kafka Record

The publisher produces this record (`src/Kafka/PartitionedKafkaPublisher.cs`) and consumers read it.

## Topic & partitioning

- **Topic**: `Kafka:Topic` (default `orders`). It MUST exist and have at least `Kafka:PartitionCount`
  (6) partitions, or the publisher stops at startup.
- **Partition**: explicit `TopicPartition(topic, Outbox.KafkaPartition)`.
  - The partition is `FNV-1a(AggregateKey) mod PartitionCount`. Kafka's default partitioner is NOT
    used.
  - All events of one order land in one partition.
- **Ordering guarantee**: within a partition, records appear in ascending `sequence-no`. No order is
  guaranteed across partitions.

## Record

| Part | Value | Status |
|------|-------|--------|
| Key | `AggregateKey` (Order: `CustomerId`, e.g. `CUS-004211`), UTF-8 string | unchanged |
| Header `event-id` | `EventId` GUID string (`D` format), UTF-8 | unchanged |
| Header `sequence-no` | `SequenceNo` decimal string, UTF-8 | unchanged |
| Header `event-type` | `OrderCreated` \| `OrderPaid` \| `OrderShipped` \| `OrderDelivered` \| `OrderCancelled` | **new values (approved)** |
| Value | JSON payload below, UTF-8 | **additive change** |

## Payload (all order event types)

```json
{
  "OrderId": "6f1c…",
  "SequenceNo": 1234,
  "OrderNumber": "ORD-0000000101",
  "CustomerId": "CUS-004211",
  "Amount": 152000.00,
  "CreatedAtUtc": "2026-11-15T08:00:00.000Z",
  "EventType": "OrderPaid",
  "PreviousStatus": "Created",
  "Status": "Paid",
  "OccurredAtUtc": "2026-11-15T08:00:03.120Z",
  "EventVersion": 1
}
```

- The first six fields keep today's names and meaning, so `OrderCreated` stays backward compatible.
- `SequenceNo` in the payload:
  - It is the **event's** sequence number, the same value as the `sequence-no` header.
  - This matches today's creation events.
- The added fields are `EventType`, `PreviousStatus` (null for `OrderCreated`), `Status`,
  `OccurredAtUtc` and `EventVersion`.
- Consumers MUST ignore unknown fields.

## Delivery semantics

- Each batch is sent in one Kafka transaction. Records of an aborted transaction are invisible to
  `read_committed` consumers.
- **At-least-once**: a crash between the Kafka commit and the SQL mark re-publishes records with the
  same `event-id`. Exactly-once is NOT provided.
- A dead-lettered event is never published, and the order's later events still flow. A consumer can
  therefore see a lifecycle gap. The run report lists such cases as dead-letter-caused, not as
  ordering violations.
