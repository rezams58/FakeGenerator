# FakeOrderGenerator Constitution

## Core Principles

### I. Per-Partition Ordering Correctness (NON-NEGOTIABLE)

- Each Kafka partition MUST be processed by exactly one independent lane.
- Outbox reads MUST be ordered by `SequenceNo ASC`, and messages in a batch MUST be published in that
  order.
- A failed message MUST block every later message of the same partition until it is either published
  or dead-lettered after `MaxRetryCount`; later messages MUST NOT overtake it.
- Partitions MUST run independently and concurrently; global ordering across partitions is NOT
  required.
- When ordering and throughput conflict, ordering wins.

Rationale: per-partition order is the core correctness guarantee of the publisher.

### II. Lease Ownership & Fencing

- Partition ownership MUST be managed in SQL Server (`dbo.PublisherPartitionLease`: `PartitionId`,
  `OwnerId`, `LeaseUntilUtc`, `FencingToken`, `UpdatedAtUtc`).
- Claim MUST be atomic; a partition MUST NOT have two valid owners at the same time.
- `SYSUTCDATETIME()` on SQL Server MUST be the only clock for lease validity.
- Every ownership transfer MUST increment `FencingToken`.
- Renew, Release and every ownership-sensitive Outbox operation (read, mark published, mark failed)
  MUST verify both `OwnerId` and `FencingToken`; operations from a stale owner MUST fail.
- When Renew fails, the publisher MUST stop sending new messages for that partition immediately.
- After an instance crashes, its partitions MUST become claimable once the lease expires.
- Kafka transactional fencing with a stable `TransactionalId` of `ClientId + Topic + PartitionId`
  MUST be kept. It MUST NOT be removed, and correctness MUST NOT rely on the SQL lease alone.

Rationale: a SQL fencing token cannot stop a zombie Kafka producer; only Kafka transactional fencing can.

### III. Kafka Commit Before SQL & Honest Delivery Semantics

- A message MUST be marked Published in SQL only after its Kafka transaction has committed.
- Marking a batch as Published MUST be one grouped (bulk) SQL operation.
- On delivery failure, the Kafka transaction MUST be aborted, and only the message that actually
  failed MUST get its `RetryCount` incremented; earlier successful messages MUST NOT be marked failed.
- Failed-event detection across concurrent deliveries (for example `Task.WhenAll`) MUST map errors
  to the correct event.
- Error details sufficient for later investigation MUST be persisted.
- The system is At-Least-Once. No code, document or report MAY claim Exactly-Once between SQL
  Server and Kafka.
- The consumer contract MUST stay documented: `IsolationLevel.ReadCommitted`, `EventId` from the
  header, an Inbox table with a unique constraint on `EventId`, and the Inbox insert plus the business
  operation in one SQL transaction.

Rationale: a crash between Kafka commit and SQL update causes re-publishing; consumers must dedupe.

### IV. Fail Fast on Topology & Configuration

- Before publishing, the worker MUST verify that the topic exists and that its real partition
  count is at least the expected count (6 for this project).
- A missing topic, too few partitions, or a metadata error MUST stop the worker; logging and
  continuing is forbidden.
- Settings (bootstrap servers, topic, partition count, ClientId / transactional namespace,
  BatchSize, LingerMs, retry limits, lease timings) MUST come from configuration, not hardcoded values.
- All instances of one deployment MUST use consistent bootstrap servers, topic, partition count and
  transactional ID namespace.

Rationale: a wrong topology silently breaks ordering and fencing guarantees.

### V. Throughput Without Compromising Correctness

- Deliveries within a batch MUST be sent concurrently; awaiting messages one by one is allowed only
  where correctness requires it.
- Partitions MUST progress independently of each other.
- `BatchSize`, `LingerMs` and producer settings MUST be configurable.
- Producer settings MUST include `Acks = All`, `EnableIdempotence = true` and
  `CompressionType = Lz4`.
- Kafka errors MUST NEVER cause a message to be recorded as Published.

Rationale: maximum throughput is the goal for 500,000 messages, but never at the cost of I–III.

### VI. Evidence-Based Verification (NON-NEGOTIABLE)

- The tests listed in `docs/spec.md` §11 (unit, SQL integration, Kafka integration, end-to-end) MUST
  exist and MUST be actually executed.
- No success of build, tests, performance or production readiness MAY be claimed without real
  execution output.
- Tests that were not run MUST be listed explicitly as not run, with the reason.
- Throughput reports MUST give real counts of generated, published, dead-lettered and duplicate
  messages.

Rationale: unverified claims are the main risk when agents implement concurrency-sensitive code.

## Technical Constraints & Scope

- Platform: .NET 8, SQL Server, Apache Kafka. Target: 500,000 messages over 6 partitions with a
  configurable number of publisher instances.
- `MaxPartitionsPerInstance` is an upper bound only; actual assignment MUST come from leases.
- SQL migrations MUST be idempotent (safe to run again) and MUST run on a clean database.
- Dead-letter behaviour MUST be kept: after `MaxRetryCount` the message gets `DeadLetterAtUtc`, and
  the next message of the partition may proceed. Changing this requires a separate, approved
  architecture decision.
- Only one Order Generator instance is supported, because `SequenceNo` is allocated with
  `MAX(SequenceNo)`. Multi-generator support MUST NOT be claimed without a separately designed atomic
  sequence allocator.
- Out of scope unless approved separately: Exactly-Once between SQL Server and Kafka; multiple
  concurrent Order Generators with the current allocator; automatic change of topic partition count;
  changes to the current message contract; deleting dead-letter messages from the database; changing
  existing consumer behaviour without a compatibility review.

## Development Workflow & Quality Gates

- Before any change: read the relevant source in full, read `src/README.md` and
  `IMPLEMENTATION_NOTES.md`, match the real SQL schema against `Data/SqlOrderRepository.cs`, and check
  current dependencies and behaviour.
- No existing method or file MAY be deleted without reviewing its usage.
- Changes MUST be small and reviewable.
- SQL errors and Kafka errors MUST be handled and reported separately.
- Every change set MUST end with a report: changed files, resulting architecture, real build and test
  output, tests not run, and residual risks.
- The acceptance criteria in `docs/spec.md` §12 are the Done gate for the publisher work.

## Governance

- This constitution overrides other project practices. Where `docs/spec.md` and this document
  conflict, this document wins until it is amended.
- Amendments MUST be made through a reviewed change that states the rationale, updates the Sync
  Impact Report, and bumps the version.
- Versioning follows semantic versioning: MAJOR for removing or redefining a principle, MINOR for a
  new principle or section or materially expanded guidance, PATCH for clarifications and wording.
- Every plan (`/speckit-plan`) and every code review MUST check compliance with the principles above;
  any deviation MUST be justified in the plan's complexity tracking.
- `docs/spec.md` is the runtime guidance for feature-level requirements.

**Version**: 1.0.0 | **Ratified**: 2026-11-15 | **Last Amended**: 2026-11-15
