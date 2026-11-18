# FakeGenerator (.NET 8) — partition-leased Kafka publisher

This worker generates synthetic orders, writes `Orders` + `Outbox` in one SQL Server transaction, and publishes Outbox rows to Kafka. The publisher supports multiple application instances with SQL partition leases and Kafka transactional producer fencing.

## Requirements

- .NET 8 SDK
- SQL Server 2016+ with database compatibility level supporting `OPENJSON`
- Kafka brokers with transactions enabled and a topic containing at least six partitions
- A disposable/test database and synthetic data only

## Database setup

Run these scripts in order against the configured database:

1. `sql/01-schema.sql`
2. `sql/03-multinstance-publisher.sql`

`sql/03-multinstance-publisher.sql` seeds lease rows for partitions `0..5`. Keep these rows and `Kafka:PartitionCount` aligned. `sql/02-monitoring.sql` contains monitoring queries. The legacy `SQLQuery1.sql` and `SQLQuery2.sql` are illustrative scripts for a different table shape and are not part of application setup.

## Kafka topic

Create the topic before starting the application:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 6 --replication-factor 1
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic orders
```

The publisher fails at startup if the topic is missing or has fewer partitions than `Kafka:PartitionCount`. It does not silently swallow topic metadata errors or create/alter the topic on every message.

## Configuration

Set `ConnectionStrings:SqlServer`, `Kafka:BootstrapServers`, and `Kafka:Topic` in `appsettings.json` or environment variables. Do not commit credentials.

Important settings:

- `Generator:TargetEventCount`: total number of **events** to generate (default 500,000). `Generator:RecordCount` is obsolete and ignored with a warning.
- `Generator:MaxActiveEntities`, `Generator:AdvanceProbability`, `Generator:RandomSeed`: size of the active order pool, chance that a slot advances an existing order instead of creating one, optional seed for repeatable runs
- `Generator:Entities:Order:PartitionKey` and `Generator:Entities:Order:Lifecycle`: partition key field and an explicit lifecycle graph (states, terminal states, weighted transitions). The graph must be acyclic; an invalid graph stops startup
- `Reporting:Directory`, `Reporting:Environment`, `Reporting:RegressionThresholdPercent`: where reports and the per-environment baseline are written, and the allowed throughput regression (default 20%)
- `Kafka:PartitionCount`: six in the current schema/migration
- `Publisher:BatchSize`: maximum Outbox rows per Kafka transaction
- `Publisher:MaxRetryCount`: after this many delivery failures, the blocking event is moved to the terminal dead-letter state (`DeadLetterAtUtc`) and later events in that partition may proceed
- `Publisher:LeaseDurationSeconds` / `LeaseRenewIntervalSeconds`: SQL lease TTL and heartbeat cadence
- `Publisher:MaxPartitionsPerInstance`: maximum partitions claimed by one process. Use `6` for one instance to run six partition lanes. For six instances, set `1` per instance; for two instances, set `3` per instance. All instances must use the same `Kafka:ClientId`, topic, and partition count so the per-partition transactional IDs are stable across owners.

The sample defaults `MaxPartitionsPerInstance` to `6`, favoring one-instance throughput. If you deploy multiple instances, adjust this value to distribute the six partitions instead of allowing the first process to claim all available partitions.

The publisher keeps checking for expired partitions while it has spare capacity, without waiting for its healthy lanes to stop. Lowering `MaxRetryCount` finalizes already-exhausted pending rows as dead letters under the current lease before selecting later events; their original error and retry count are preserved.

## Run

```bash
dotnet restore
dotnet build
dotnet test
dotnet run
```

## Multi-instance coordination and ordering

- `PublisherPartitionLease` assigns each partition to one owner. Claims are atomic SQL updates using row locks and `READPAST`.
- SQL Server UTC is authoritative for lease expiry. Each lease has an incrementing `FencingToken`; renewal, success updates, failure updates, and release are conditional on owner + token + unexpired lease.
- Each partition uses its own Kafka producer with a stable `transactional.id` (`ClientId` + topic + partition). When a new owner initializes transactions, Kafka fences the previous producer for that partition. A stale owner also stops when its lease heartbeat fails.
- A partition's Outbox rows are read and enqueued in ascending `SequenceNo`; different partitions run concurrently. One partition never intentionally advances beyond a failed head event. Once the head reaches `MaxRetryCount`, it is marked dead-lettered and the next event may proceed. This means the main topic order excludes terminal dead-lettered events.
- A Kafka batch is a Kafka transaction. The producer queues records in sequence order and awaits all delivery reports before committing the transaction. If a record delivery fails, the failed transaction is aborted; the successful prefix is retried/committed separately and only the actual failing event's retry count is incremented.
- The SQL `PublishedAtUtc` update happens after Kafka transaction commit. A crash between those operations can cause replay. This is **at-least-once**, not cross-system exactly-once. Consumers must deduplicate by `event-id`.
- Consumers that must hide aborted Kafka transactions must use `isolation.level=read_committed` (Confluent.Kafka: `IsolationLevel = IsolationLevel.ReadCommitted`). A consumer using `read_uncommitted` can observe records from aborted transactions.

### Consumer deduplication pattern

Create an Inbox table with a unique key on `EventId`. In one SQL transaction, insert the event ID and apply the business change. If the unique key already exists, treat the event as already processed and commit without repeating the business effect. The Inbox insert and business change must be in the same transaction.

Example schema:

```sql
CREATE TABLE dbo.ConsumerInbox
(
    EventId UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_ConsumerInbox PRIMARY KEY,
    ProcessedAtUtc DATETIME2(3) NOT NULL CONSTRAINT DF_ConsumerInbox_ProcessedAtUtc DEFAULT SYSUTCDATETIME()
);
```

## Lifecycle, delivery semantics and consumers

- Orders move Created -> Paid -> Shipped -> Delivered; Cancelled is possible only from Created or Paid. Every transition is one Outbox event, written in the same SQL transaction as the order change.
- Delivery is **at-least-once**. Kafka commits before the SQL mark, so a crash in between replays the batch with the same `event-id`. Consumers must deduplicate by `event-id` with an Inbox table (see `specs/001-fake-data-kafka-publishing/contracts/consumer-inbox.md`; reference code in `tests/FakeGenerator.Tests/Kafka/InboxConsumer.cs`).
- `Kafka:PartitionCount` must not change after events exist (it defines the partition of every key).
- Only **one generator instance** may run at a time (it allocates `SequenceNo` from `MAX(Outbox.SequenceNo)`).
- All publisher instances must share the same bootstrap servers, topic, partition count and `Kafka:ClientId`. Different values are unsupported (the transactional id depends on them).

## Verify and run report (`--verify`)

After a generate + publish run:

```bash
dotnet run --project src -- --verify
```

It reads the whole topic with a `read_committed` consumer, checks duplicates, per-partition `sequence-no` order, per-order lifecycle paths, dead-letter gaps and events missing on the stream (compared with SQL), then writes `reports/run-{utc}.json` and compares throughput with `reports/baseline.{Environment}.json` (created on the first full run). Exit code is non-zero on any violation or regression above the threshold.

Expected at-least-once replays are counted as duplicates without failing verification. The Inbox consumer must still deduplicate them. A throughput baseline is created only for a complete run whose verification passed.

Verification succeeds only when the configured target is reached, every event is published or dead-lettered, and every captured Kafka partition end is reached. `VerifierComplete=false` fails the run. Performance baselines apply only to complete 500,000-event samples. `GeneratedThisRun` is null unless a generation-start count was explicitly captured; the verifier does not infer it from another report or database.

Outbox reads take a short shared table lock while selecting an ordered batch. This prevents concurrent generator inserts from appearing partway through a read and overtaking a lower sequence. The lock is released before Kafka publishing; benchmark throughput on the target SQL Server deployment.

## Adding a new entity

A new fake entity type needs only these four steps. The generator pipeline (`src/Generation/EventBatchBuilder.cs`, `src/Workers/`) is not changed.

1. **Definition**: implement `IFakeEntityDefinition` in `src/Generation/<Entity>/` (name, `LifecycleDefinition`, partition key fields, `CreateNew`, `BuildPayload`).
2. **Writer**: implement `IEntityWriter` in the same folder (insert, apply transition, load active entities for restart).
3. **Migration**: add an idempotent `src/sql/NN-<entity>.sql` creating the entity table with `Status`, `LastEventSequenceNo`, `UpdatedAtUtc`.
4. **Config**: add an `Generator:Entities:<Entity>` entry in `appsettings.json` (`PartitionKey`, optional explicit `Lifecycle`) and register the definition and writer in `Program.cs`.

`tests/FakeGenerator.Tests/Unit/SampleEntity/` holds a test-only example (a "Ticket" entity with a different partition key and a 3-state lifecycle).

## Tests

`tests/FakeGenerator.Tests` contains deterministic partition-mapping tests and an opt-in SQL lease concurrency/fencing test. To run the SQL test, set `FAKEGEN_TEST_SQL` to a disposable database after applying both setup scripts. The SQL test claims all six partitions concurrently from two owners, expires one lease, verifies takeover increments the fencing token, and verifies the stale token cannot renew.

## Operational limitations to verify in your environment

- Kafka transaction support and broker `transaction.max.timeout.ms` must be configured appropriately.
- `transactional.id` uniqueness depends on all instances sharing the same `Kafka:ClientId`, topic, and partition mapping. Do not deploy two logically different applications with the same ID namespace.
- Increasing Kafka partition count changes key-to-partition mapping for hash-based partitioning; treat that as a data migration, not a casual runtime setting change.
- `SequenceNo` is currently global and allocated by the single generator worker using the current Outbox maximum. Running multiple generator workers concurrently is not supported by this sequence allocator.
- SQL/Kafka is not a distributed transaction. Consumer idempotency remains mandatory.
