# Quickstart: Validate Fake Data Generation and Ordered Publishing

This guide runs and validates the feature. It does not describe the implementation. For the settings
see [contracts/configuration.md](./contracts/configuration.md); for the record format see
[contracts/kafka-message.md](./contracts/kafka-message.md).

## Prerequisites

- .NET 8 SDK.
- A disposable SQL Server database.
- A Kafka cluster with transactions enabled. For a single broker,
  `transaction.state.log.replication.factor` and `min.insync.replicas` must be set to fit.
- Kafka CLI tools, or any admin UI.

## 1. Database

Run the scripts in this order on a **clean** database. Then run them all a second time. The second
run must succeed with no changes, because the migrations are idempotent.

```text
src/sql/01-schema.sql
src/sql/03-multinstance-publisher.sql
src/sql/04-lifecycle.sql
src/sql/02-monitoring.sql   (optional)
```

## 2. Topic

```bash
kafka-topics.sh --bootstrap-server <servers> --create --topic orders --partitions 6 --replication-factor 3
```

Negative check: point `Kafka__Topic` at a topic with 3 partitions. The publisher must stop with a
topology error and publish nothing (SC-006).

## 3. Build & unit tests

```bash
dotnet build FakeGenerator.sln -c Release
dotnet test tests/FakeGenerator.Tests -c Release
```

Expected: all unit tests pass. SQL and Kafka tests show as skipped, which means **not run**, until
the environment variables below are set.

## 4. Integration & E2E tests

```bash
export FAKEGEN_TEST_SQL="<connection string of the disposable DB>"
export FAKEGEN_TEST_KAFKA="<bootstrap servers>"
dotnet test tests/FakeGenerator.Tests -c Release
```

Expected: the SQL lease, Kafka transaction and fencing, and E2E groups pass (research.md R9).

## 5. Manual end-to-end run (500K events, two instances)

1. **Instance A** (generator + publisher). The publisher can claim up to 3 partitions:
   ```bash
   dotnet run --project src -c Release -- --Generator:TargetEventCount=500000 --Publisher:MaxPartitionsPerInstance=3
   ```
2. **Instance B** (publisher only, in a second terminal):
   ```bash
   dotnet run --project src -c Release -- --Generator:Enabled=false --Publisher:MaxPartitionsPerInstance=3
   ```
3. While the run is in progress, **kill instance B** (`taskkill /F` or `kill -9`). Instance A is capped at 3, so start
   a new instance C (same command as B). B's partitions must resume within
   one lease duration plus one renewal interval (≈110 s by default) (SC-003).
4. When the generator stops and the publishers become idle, run the verifier:
   ```bash
   dotnet run --project src -c Release -- --verify
   ```

## 6. Verification queries (SQL)

```sql
-- Total = target, nothing left pending (SC-001)
SELECT COUNT(*) AS Total,
       SUM(CASE WHEN PublishedAtUtc  IS NOT NULL THEN 1 ELSE 0 END) AS Published,
       SUM(CASE WHEN DeadLetterAtUtc IS NOT NULL THEN 1 ELSE 0 END) AS DeadLettered,
       SUM(CASE WHEN PublishedAtUtc IS NULL AND DeadLetterAtUtc IS NULL THEN 1 ELSE 0 END) AS Pending
FROM dbo.Outbox;

-- All events of an order in one partition (expect 0 rows)
SELECT JSON_VALUE(Payload, '$.OrderId') AS OrderId
FROM dbo.Outbox
GROUP BY JSON_VALUE(Payload, '$.OrderId')
HAVING COUNT(DISTINCT KafkaPartition) > 1;

-- Lifecycle distribution and incomplete lifecycles
SELECT Status, COUNT(*) FROM dbo.Orders GROUP BY Status;

-- Ownership after takeover (tokens increased, each partition has one owner)
SELECT PartitionId, OwnerId, LeaseUntilUtc, FencingToken FROM dbo.PublisherPartitionLease;
```

## 7. Expected outcomes

| Check | Expected |
|-------|----------|
| Outbox total | exactly `TargetEventCount` (500,000) |
| Pending after the run | 0 |
| Verifier `orderingViolations` / `lifecycleViolations` | 0 / 0 (SC-002, SC-010) |
| Verifier `missingOnStream` | 0 |
| Verifier `duplicates` | ≥ 0. Above 0 only when a crash happened after a commit, and every duplicate shares an `event-id` (SC-004) |
| Run report | `reports/run-*.json` with real counts and throughput; on the first run, the baseline file is created (SC-007, SC-009) |
| Invalid lifecycle config (e.g. a cycle Paid→Created) | The generator refuses to start, and the Outbox row count is unchanged |

Record every check that was not executed as **not run**, with the reason (Constitution VI).
