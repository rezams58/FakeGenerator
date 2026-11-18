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
