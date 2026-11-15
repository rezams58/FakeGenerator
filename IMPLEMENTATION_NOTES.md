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

## Validation status

- JSON and project XML syntax checks passed in the available environment.
- Static source checks for balanced delimiters and obsolete publisher method references passed.
- `dotnet restore`, `dotnet build`, `dotnet test`, SQL migration execution, and Kafka/SQL integration tests were **not run** because the .NET SDK, SQL Server client/engine, and Kafka broker are not available in this environment.
- The SQL concurrency test is opt-in through `FAKEGEN_TEST_SQL` and must be run against a disposable database after applying `sql/01-schema.sql` and `sql/03-multinstance-publisher.sql`.

This revision is an implementation candidate, not a claim of production readiness. Run the full build and integration matrix in an environment with .NET 8, SQL Server, and Kafka before deployment.
