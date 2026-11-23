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

## v2 acceptance scenarios
Use fresh isolated SQL and six-partition ingress/final topics. Validate fingerprint
fixtures and strict verifier first; then run finalizer integration and deterministic
failure scenarios listed in tasks T064–T084. Close the run, drain both stages, and
verify exactly 500000 committed final records/IDs, with all strict errors zero.
No runnable finalizer command is claimed until its build/configuration tasks finish.

## v2 planned validation runbook (Phase 1)

The following commands are the implementation acceptance interface, not commands
claimed runnable today. Do not run until the corresponding tasks create the service,
migrations and CLI. Use disposable resources and process-local credentials.
Prerequisites: .NET8, Java17, Maven, supported Kafka with topic UUIDs/transactions,
SQL test catalog, six-partition fresh ingress/final topics, retention covering all
history, and provisioner/runtime/verifier roles as contracts/v2-processing.md specifies.
Provisioning must register exact topic IDs/internal topics and boundaries before input.

```powershell
# Build and fixtures/integration tests after implementation
 dotnet test tests/FakeGenerator.Tests -c Release
 mvn -f services/finalizer/pom.xml verify
# Planned service preflight and process (use a separate terminal per process)
 java -jar services/finalizer/target/finalizer.jar --config local-finalizer.properties --check-only
 java -jar services/finalizer/target/finalizer.jar --config local-finalizer.properties
# UUID comes from explicit pipeline registration; save returned run UUID
 dotnet run --project src -c Release -- --new-run --target-events 500000 --pipeline-id <pipeline-uuid>
 dotnet run --project src -c Release -- --verify --run-id <returned-run-uuid>
```

Do not place secrets in arguments or tracked properties. Missing SQL/Kafka/JVM tests
are explicitly not run, not passed. Package must set finalizer.jar as its artifact name.

Validation order:
1. Compare canonical fixtures in C#/Java, including decimal precision and invalid input.
2. Migrate fresh/legacy databases twice; interrupt final generation transaction and
   resume the same RunId. New run in nonempty DB generates its own target only.
3. Inject duplicate/conflicting/wrong-partition ingress; ensure no invalid final forward.
4. Fault injection at ingress commit/SQL mark and finalizer commit boundaries, then
   rebalance and erase local state with intact changelog; verify restoration.
5. On separate disposable pipelines, test changed application.id, deleted/recreated
   internal topics and unavailable registry/catalog; fail before new output.
6. Drain and verify 500000 exact final events, zero strict errors. Repeat another run
   and audit each explicitly; classify known other-run records separately.
7. Recovery controller uses monotonic termination-to-final-observation bounds on all
   affected partitions. >18 seconds fails. Persist real logs/counts and versioned
   final-throughput baseline only on strict pass. Consumer cutover requires approval.

Until these checks execute, no v2 throughput, recovery or exactly-once runtime success
is claimed. Next workflow is speckit-tasks followed by speckit-analyze.

### I3 ordered migration validation
After 01-schema.sql, 03-multinstance-publisher.sql and 04-lifecycle.sql, the planned
v2 order is 05-pipeline-registration.sql -> 06-generation-runs.sql ->
07-verification-snapshots.sql. Repeat on clean and legacy fixtures; require idempotency
and rejection of a GenerationRun with an unknown PipelineId. These scripts are pending
implementation; this documentation change does not claim database execution.

### U7 recovery validation
Before T097, prove supported lifecycle integration using T096. Inject old-owner commit
and abort while takeover starts; rebalance during restore, boundary capture, scan and
gate-open; release stale audit callbacks after reassignment. Require no forwarding
before the current token validates and no committed stale-owner output. Record H/C,
topic IDs, transition times and assignment token; timeouts are incomplete/failed
recovery, never successful audits. U8/U9/C1 are not resolved by this scenario.

### U8 verification completion checks
With selected run closed and SQL pending zero, deliberately hold the finalizer behind:
verification must wait. Advance only its broker-committed offsets/output atomically,
then verify strict equality. Include control-only/aborted tails, unresolved transactions,
coordinator/ingress crash and cancellation. The barrier remains held after coordinator
crash until explicit recover/release. Record epoch and H/B/F vectors; timeout defaults
to 600 seconds for the whole attempt and exits 3 with no baseline. Do not confuse it
with 18-second recovery acceptance. These are planned tests, not executed results.
