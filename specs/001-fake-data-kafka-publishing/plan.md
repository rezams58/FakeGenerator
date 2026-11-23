# Implementation Plan: Fake Data Generation and Ordered Event Publishing

**Branch**: `001-fake-data-kafka-publishing` | **Date**: 2026-11-15 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-fake-data-kafka-publishing/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

Extend the existing .NET 8 worker (`src/`) so that it:

1. Generates fake data through an **entity definition** abstraction. Order is the first entity, and the
   pipeline stays entity-agnostic (FR-004, SC-008).
2. Drives each order through a **configurable lifecycle state machine**:
   - Default path: Created → Paid → Shipped → Delivered.
   - Cancelled is allowed only from Created or Paid.
   - Each state change writes one Outbox event in the same SQL transaction as the record change
     (FR-002, FR-023, FR-024).
3. Counts the **target in events** (default 500,000) and stops exactly at the target (FR-001).
4. **Fixes the publisher**. Its constructor never assigns its settings fields, so it never runs. The
   fix keeps the existing outbox, SQL lease and fencing token, and the per-partition Kafka
   transactional producer (FR-005 to FR-018).
5. Produces a **run report** that combines:
   - SQL counts.
   - A read-committed verification consumer that counts duplicates and checks ordering and lifecycle
     validity.
   - A throughput baseline comparison (FR-021, SC-007, SC-009, SC-010).

Note: `docs/spec.md` says "500,000 orders". The clarified spec (Q5) counts **500,000 events**, and the
clarified spec governs this feature.

## Technical Context

**Language/Version**: C# 12 / .NET 8 (`net8.0`)

**Primary Dependencies**:
- Confluent.Kafka 2.6.1
- Microsoft.Data.SqlClient 5.2.2
- Microsoft.Extensions.Hosting 8.0.1
- Microsoft.Extensions.Options.ConfigurationExtensions 8.0.0
- No new runtime packages are planned.

**Storage**: SQL Server:
- `dbo.Orders`
- `dbo.Outbox`
- `dbo.PublisherPartitionLease`
- a new idempotent migration `src/sql/04-lifecycle.sql`

**Testing**: xunit 2.9.2 + Microsoft.NET.Test.Sdk 17.11.1.
- Unit tests always run.
- SQL integration tests are opt-in via `FAKEGEN_TEST_SQL`. This pattern already exists.
- Kafka integration and E2E tests are opt-in via `FAKEGEN_TEST_KAFKA`, against a real broker.

**Target Platform**: .NET 8 worker service (Windows/Linux), several instances per deployment

**Project Type**: Background worker (single project) + test project

**Performance Goals**:
- No fixed minimum.
- Every run reports throughput.
- The first full 500K run per environment is the baseline.
- A run more than 20% slower than the baseline fails (SC-009).

**Constraints**:
- Ordering beats throughput.
- At-least-once delivery.
- Kafka commit happens before the SQL mark.
- Single generator instance (`MAX(SequenceNo)` allocator).
- All settings come from configuration.
- Migrations are idempotent.

**Scale/Scope**:
- 500,000 events over 6 partitions.
- N publisher instances, each with `MaxPartitionsPerInstance` as an upper bound.
- One entity type (Order), plus one test-only sample entity for SC-008.

No `NEEDS CLARIFICATION` remains. All design decisions are in [research.md](./research.md).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | How the design complies | Pre | Post |
|---|-----------|-------------------------|-----|------|
| I | Per-Partition Ordering (NON-NEGOTIABLE) | One lane per partition. Reads are `ORDER BY SequenceNo`. A failed event blocks its partition until it is published or dead-lettered. An order's events share one key, so they share one partition, and get increasing global `SequenceNo` in lifecycle order (R4). | PASS | PASS |
| II | Lease Ownership & Fencing | The lease table, `SYSUTCDATETIME`, the token increment and owner+token checks are kept unchanged. The TransactionalId `{ClientId}-{Topic}-partition-{id}` is kept. Lease rows are ensured from the configured partition count (R6). | PASS | PASS |
| III | Kafka Commit Before SQL & At-Least-Once | `MarkPublishedAsync` (bulk) runs only after `CommitTransaction`. Only the failed event gets `RetryCount+1`. The consumer Inbox contract is documented in [contracts/consumer-inbox.md](./contracts/consumer-inbox.md). Exactly-once is never claimed. | PASS | PASS |
| IV | Fail Fast on Topology & Configuration | `KafkaTopicValidator` runs before publishing. New startup validation covers Publisher options and lifecycle config (R2, R3). The generator refuses to start on an invalid lifecycle. Nothing is hardcoded. | PASS | PASS |
| V | Throughput Without Compromising Correctness | Concurrent `ProduceAsync` + `WhenAll` per batch is kept. Producer settings are kept: Acks=All, idempotence, Lz4. The batch and linger values are configurable. | PASS | PASS |
| VI | Evidence-Based Verification (NON-NEGOTIABLE) | All test groups from docs/spec.md §11 are planned (R9). The run report gives real counts from SQL and the verifier (R8). Tests that were not run must be listed. | PASS | PASS |

The scope constraint "changes to the current message contract" is out of scope unless approved
separately. The Outbox `EventType` set and the payload change from creation-only to lifecycle events.
The user approved this change in the clarify session (spec Clarifications, 2026-11-15). It is recorded
in Complexity Tracking.

**Gate result**: PASS. One justified deviation, listed below.

## Project Structure

### Documentation (this feature)

```text
specs/001-fake-data-kafka-publishing/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   ├── kafka-message.md     # Kafka record contract (key, headers, payload per event type)
│   ├── configuration.md     # appsettings contract + validation rules
│   └── consumer-inbox.md    # Consumer dedup / ordering / lifecycle contract
├── checklists/requirements.md
└── tasks.md             # Phase 2 output (/speckit-tasks, not created here)
```

### Source Code (repository root)

```text
src/
├── Configuration/
│   ├── GeneratorSettings.cs             # + TargetEventCount, Entities/Lifecycle options, ReportingConfig
│   └── OptionsValidators.cs             # NEW — ValidateOnStart rules from contracts/configuration.md
├── Data/
│   ├── Partitioning.cs                  # FNV-1a GetPartition (reused); records Order/OutboxMessage
│   └── SqlOrderRepository.cs            # + lifecycle writes, EnsureLeasePartitions, report queries
├── Generation/                          # NEW — entity-agnostic generation
│   ├── IFakeEntityDefinition.cs         # fields, partition key, lifecycle, payload factory
│   ├── IEntityWriter.cs                 # per-entity state persistence inside the shared transaction
│   ├── LifecycleDefinition.cs           # state machine + validation + random valid next step
│   ├── EventBatchBuilder.cs             # mixes new entities / transitions, stops at target
│   └── Order/OrderDefinition.cs, OrderWriter.cs
├── Kafka/
│   ├── IPartitionPublisher.cs           # NEW — test seam over PartitionedKafkaPublisher (+ IOutboxRepository seam over the repository)
│   ├── KafkaTopicValidator.cs           # unchanged
│   └── PartitionedKafkaPublisher.cs     # unchanged contract (headers, TransactionalId)
├── Reporting/                           # NEW
│   ├── RunReport.cs, RunReportWriter.cs # SQL counts + throughput + baseline comparison
│   └── StreamVerifier.cs                # read_committed consumer: duplicates, ordering, lifecycle
├── Models/OrderEventPayload.cs          # NEW lifecycle payload; replaces unused OrderEvent.cs (after usage review)
├── Workers/
│   ├── GeneratorWorker.cs               # NEW generic worker over entity definitions; replaces OrderGeneratorWorker.cs
│   └── KafkaPublisherWorker.cs          # constructor fix; logic kept
├── sql/04-lifecycle.sql                 # NEW idempotent migration
└── Program.cs                           # options validation, DI for definitions, --verify mode

tests/FakeGenerator.Tests/
├── Unit/        # lifecycle validation & paths, partition mapping, batch builder target stop,
│                # retry/dead-letter, failed-event mapping, transactional id, sample entity (SC-008)
├── Sql/         # existing lease concurrency + takeover, token increment, stale ops (FAKEGEN_TEST_SQL)
└── Kafka/       # commit/abort, fencing, ordering, parallel partitions, read_committed, E2E (FAKEGEN_TEST_KAFKA);
                 # InboxConsumer.cs = reference consumer for the dedup E2E (contracts/consumer-inbox.md)
```

**Structure Decision**:
- Keep the single worker project and the single test project.
- Add the `Generation/` and `Reporting/` folders inside `src/`.
- Do not add new projects. The verifier runs as a mode of the same executable (`--verify`), so
  deployment stays one artifact.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Message contract change: new `EventType` values (`OrderPaid`, `OrderShipped`, `OrderDelivered`, `OrderCancelled`) and a lifecycle payload | Required by FR-023/FR-024. The user explicitly approved it in clarify (Q3) | Creation-only events cannot show lifecycle ordering within a partition, which is the core feature goal. The headers and key stay unchanged, and `OrderCreated` stays compatible (additive fields only; see contracts/kafka-message.md) |

## v2 implementation amendment (approved 2026-11-22)

This section supersedes v1 no-new-runtime/at-least-once-final assumptions above.
Architecture: .NET/SQL Outbox -> six-partition ingress -> Kafka Streams JVM service
with exactly_once_v2 and persistent logged dedup store -> six-partition final topic.
See contracts/final-topic-v2.md and ../../docs/exactly-once-final-topic-design.md.

New service: services/finalizer/ (Maven, Java, Streams); tests in its src/test/java/.
Select and pin JDK/Streams versions after recording actual broker compatibility in
research.md; no compatibility or performance success is assumed before execution.
.NET verifier/report changes stay in src/Reporting/; run migration in src/sql/;
fault orchestration in tests/e2e/. Persistent run identity must be loaded on restart.

Constitution v2 gates: ingress SQL leases/fencing and commit-before-mark retained;
finalizer uses consumer-group ownership and Streams transactional fencing; no SQL
lease for its state store. Final output/state/offsets commit atomically. Only the
finalizer writes final topics. Never count duplicate ingress as final success.

Complexity: approved extra JVM service and state changelog are required to remove
application-level replay in final Kafka records. No cross-system atomicity claimed.
Ordering/retention/restore/finalizer ACL checks precede acceptance and cutover.
Implementation sequence: setup -> strict verifier (US5) -> pipeline (US6) -> failure
and migration tests (US7) -> full benchmark/documentation. No production cutover in
this documentation stage. First independently useful slice is strict verification.

## Current v2 Phase 0/1 design — 2026-11-22

This section and contracts/v2-processing.md supersede preceding v1 and draft-v2
mechanism descriptions. Actual git branch during planning: main. setup-plan resolves
feature context 001-fake-data-kafka-publishing independently of the git branch.

Technical context: existing C#/.NET8 SQL ingress; new Java17/Maven Streams4.1.2 design
profile, EOS v2, JDBC read-only SQL catalog access and persistent logged state. Select
and pin a Java17-compatible Microsoft JDBC driver during dependency verification;
actual environment support is a deployment gate, not an assumed successful test.
Strict startup needs cluster/topic UUIDs, six partitions and registered internal topics.

Phase 0 decisions R10–R16 resolve the design questions without changing constitution.
Phase 1 artifacts: contracts/v2-processing.md, final-topic-v2.md, data-model.md and
quickstart.md. Tasks must next be regenerated to include FR-032–037 and SC-014–016.
No implementation may begin using the obsolete dependency chain T064 -> ... -> US5
until speckit-tasks and speckit-analyze synchronize and verify the new foundations.

Foundation order: dependency/fixture contract -> registry/audit schema + tests ->
immutable event/run model + tests -> ingress/final settings and RunId propagation ->
strict verifier -> finalizer topology/catalog/restore guard -> failure tests -> full run.
Thus the old T075 responsibilities move before US5 integration tests. Tests precede
implementation; foundation includes closed-run migration and restart interruption tests.

Planned modules: src/Reporting/EventCanonicalizer.cs; src/Data/GenerationRunRepository.cs;
src/Data/PipelineRegistrationRepository.cs; services/finalizer/src/main/java/fakegen/
EventCanonicalizer.java, CatalogValidator.java, PipelineGuard.java and existing planned
FinalizerTopology/FinalizerApplication. JDBC catalog calls are bounded and errors halt
forwarding without committing offsets. Keep error domains and metrics separate.

### Constitution gate (design, not runtime evidence)

I PASS: original per-partition routing and ordering retained; bad events block output.
II PASS: ingress SQL leases/fencing unchanged; final ownership uses approved Streams model.
III PASS: ingress commit-before-mark; final state/output/offsets atomic; no SQL distributed
transaction claim. External-effect Inbox remains documented.
IV PASS: exact-six topology/identity/config preflight; incompatible history fails closed.
V PASS: batching/concurrent ingress, independent tasks, acks/idempotence/LZ4 retained.
VI PASS for planning: evidence is not fabricated; all runtime acceptance remains pending.
Post-design gate: PASS to task generation, NOT authorization to deploy or claim correctness.

Complexity: added read-only SQL catalog dependency and restore audit are necessary to
validate cross-partition identity and state loss. SQL outage reduces availability;
full output/state audit may exceed recovery target. Benchmark failure requires redesign
through Spec Kit, not relaxing the target. No arbitrary state eviction is permitted.


### I3 migration ordering correction — 2026-11-23

After existing migrations 01/03/04, apply 05-pipeline-registration.sql (T089),
then 06-generation-runs.sql (T066), then 07-verification-snapshots.sql (T093).
GenerationRun.PipelineId references the already-created PipelineRegistration table.
T066 requires T089; T088 follows T066. Registry schema creation requires no run row
and no Kafka bootstrap. Runtime provisioning remains T090. Preserve task IDs.
Migration tests apply this sequence twice on clean and explicitly mapped legacy data,
and verify unknown PipelineId rejection. No migration was executed in this design fix.

### U7 recovery ordering correction — 2026-11-23
Use the assignment-scoped protocol in contracts/v2-processing.md: Streams-owned
ownership/fencing readiness -> complete committed restoration -> stable H/C boundaries
-> audit -> serialized ownership recheck -> forwarding. Any rebalance invalidates the
entire token-bound result. Do not infer readiness from RUNNING or one store callback.
T096 proves supported lifecycle integration before T097; inability to establish it is
a planning blocker, not permission for a custom producer to manipulate Streams IDs.
No runtime correctness or 18-second recovery success is claimed by this document.

### U8 final completion barrier — 2026-11-23
Verification uses the pipeline-wide epoch barrier in contracts/v2-processing.md.
New-run admission/ingress batches pause, current ingress transactions and SQL marks
resolve, then a read_committed observer captures a stable ingress cut. Registered
finalizer broker-committed offsets must cover the last committed business record in
each partition before final output boundaries are audited. Control-marker offsets
are not treated as business offsets. Snapshot evidence is persisted before release.
Reporting.VerificationTimeoutSeconds (default 600) bounds the entire attempt; incomplete
attempts exit 3 and cannot create a baseline. Recovery timing remains a separate gate.
New shared module: src/Reporting/VerificationBarrierCoordinator.cs. Barrier state and
lane acknowledgements live in SQL; ingress workers consult it at batch admission and
on takeover. A coordinator crash leaves it closed pending explicit recovery/release.
T069 owns barrier integration and T071 proves no premature success with a lagging finalizer.
This correction does not settle U9 fault isolation or C1 reverse-state comparison.

### U9 gate correction — 2026-11-23
Post-design constitution gate for principle I/V partition independence is OPEN: the
shared-thread error isolation mechanism has not been established. This supersedes the
earlier unconditional PASS for that aspect; no principle is weakened. Handler FAIL or
CONTINUE is insufficient for local no-loss retry. See contracts/v2-processing.md U9.
Before finalizer implementation, T101 must produce supported isolation evidence and a
concrete retry/abort policy. Scope changes (extra processes/topics/topology) require
approval. After evidence, update this plan and rerun analysis before marking gate PASS.
