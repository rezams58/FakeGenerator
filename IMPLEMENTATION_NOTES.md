# Implementation notes — multi-instance publisher

## Changed in this revision

- Replaced the shared, unclaimed Outbox polling loop with SQL-backed partition lease claims.
- Added `PublisherPartitionLease` ownership, expiry, heartbeat, and monotonically increasing fencing tokens.
- Fenced stale Kafka producers with one stable Kafka `transactional.id` per topic/partition. All deployed instances must share the same `Kafka:ClientId` namespace.
- Added lease checks to Outbox reads and to success/failure SQL updates.
- Changed publishing to one producer lane per owned partition. Records are enqueued in `SequenceNo` order, awaited as a batch, and committed as a Kafka transaction.
- On a delivery failure inside a batch, the aborted batch's successful prefix is committed separately; only the actual failed event receives a retry increment.
- Added terminal dead-letter state (`DeadLetterAtUtc`) after `MaxRetryCount`, without deleting the event.
- Added startup checks for topic metadata/partition count and the SQL lease rows.
- Removed per-message topic administration and the previous code that swallowed topic creation errors.
- Set producer acknowledgements to `All`, idempotence on, LZ4 compression, and configurable batching/linger.
- Removed temporary ServiceProvider construction from `Program.cs`.
- Added SQL lease migration, corrected monitoring queries for the current schema, and added unit/opt-in SQL concurrency tests.

## Important semantics

- Per-partition ordering is maintained for non-dead-lettered events. A terminal dead-letter event is skipped so later events can proceed; therefore it is not present in the main topic stream.
- Kafka transactions fence stale producers and make each Kafka batch atomic within Kafka. They do not create a distributed transaction with SQL Server.
- If Kafka commits but SQL does not update `PublishedAtUtc`, the event can be replayed. Delivery remains at-least-once; consumers must atomically deduplicate on `EventId` with their business write.
- SQL fencing protects database operations. Kafka transactional producer fencing is also used because a SQL token alone cannot prevent a stale process from writing to Kafka.
- The current order generator still uses `MAX(SequenceNo)` and is not safe to run in multiple generator instances. This revision targets multi-instance publisher ownership, not multi-instance order generation.

## Validation status (updated from real output)

- `dotnet build FakeGenerator.sln -c Release`: 0 warnings, 0 errors.
- `dotnet test tests/FakeGenerator.Tests -c Release`: **94 passed, 13 skipped, 0 failed**.
- **Not run** (skipped because `FAKEGEN_TEST_SQL` / `FAKEGEN_TEST_KAFKA` are not set; no SQL Server or Kafka broker was available):
  - SQL: `MigrationIdempotencyTests`, `GeneratorAtomicityTests`, `GeneratorRestartTests`, `OutboxSelectionTests`, `LeaseLifecycleTests` (2 tests), `SqlLeaseConcurrencyTests`.
  - Kafka: `PublisherKafkaTests`, `TopicValidationTests` (missing topic, too few partitions), `FencingTests`.
  - Both variables: `TwoInstanceE2ETests`, `ConsumerDeduplicationE2ETests`.
- Not exercised against a real database: OPENJSON `DATETIME2(3)` parsing, the lease-partition CTE in `EnsureLeasePartitionsAsync`, the filtered index in `04-lifecycle.sql`, and the `--verify` stream read (`StreamVerifier` I/O; its logic is unit-tested in `StreamVerifierCore`).
- The 500K-event E2E run (quickstart) and the docs/spec.md section 12 acceptance check were **not run**.

This revision is an implementation candidate, not a claim of production readiness. Run the full build and integration matrix with .NET 8, SQL Server, and Kafka before deployment.
