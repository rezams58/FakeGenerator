# Research: Fake Data Generation and Ordered Event Publishing

Phase 0 output for [plan.md](./plan.md). Each item: Decision / Rationale / Alternatives considered.
Every finding comes from reading the source at baseline commit `62bcc36`.

## R1. Publisher never runs (constructor bug)

**Finding**: In `src/Workers/KafkaPublisherWorker.cs`, the constructor assigns only `_repository`,
`_publisherSetting`, `_logger` and `_kafkaSetting`. The fields `_enabled`, `_batchSize`, `_idleDelay`,
`_retryDelay`, `_maxRetryCount`, `_leaseSeconds`, `_renewSeconds` and `_maxPartitions` keep their
defaults, so `if (!_enabled) return;` exits immediately. The class also overrides `Equals` and
`GetHashCode` with no purpose.

- **Decision**:
  - Read every value from `IOptions<PublisherConfig>` in the constructor.
  - Remove the meaningless `Equals`/`GetHashCode` overrides, after a usage review.
  - Leave the batch/lease logic unchanged apart from the items in R7.
- **Rationale**: This is the smallest change that makes the publisher run (Constitution: small,
  reviewable changes). The ordering, fencing and commit-before-mark logic is already in line with
  principles I–III.
- **Alternatives considered**: Rewriting the worker. Rejected: it carries a high regression risk on
  concurrency-sensitive code.

## R2. Configuration validation (fail fast)

**Finding**: `Program.cs` validates the Kafka and Generator options but not `PublisherConfig`. The
generator reads `Generator:RecordCount`/`TotalOrders` through raw `IConfiguration` with a default of
500000, while `GeneratorConfig.RecordCount` defaults to 50000 and appsettings sets 50000. The
settings `Min/MaxOrderAmount`, `CustomerCount` and `OrderHistoryDays` are never used.

- **Decision**:
  - Use only strongly typed options with `ValidateOnStart`:
    - `PublisherConfig`:
      - BatchSize > 0
      - MaxRetryCount ≥ 1
      - MaxPartitionsPerInstance ≥ 1
      - 0 < LeaseRenewIntervalSeconds < LeaseDurationSeconds
    - `GeneratorConfig`:
      - `TargetEventCount` > 0
      - BatchSize > 0
      - Min ≤ Max amount
      - CustomerCount > 0
  - Replace `RecordCount` with `TargetEventCount` (default 500000). Accept `RecordCount` as an
    obsolete alias that logs a warning.
  - Make the unused settings drive the Order field generators.
- **Rationale**: FR-022 and Constitution IV forbid hardcoded values and require a refusal to start
  on bad configuration. Having one source of truth removes the 50K/500K conflict.
- **Alternatives considered**: Keep raw `IConfiguration` reads. Rejected: they are untyped and fail
  silently.

## R3. Lifecycle state machine

- **Decision**:
  - Each entity has a `LifecycleDefinition` built from configuration with these parts:
    - `InitialState`
    - `States`
    - `TerminalStates`
    - `Transitions` as `{From, To, Weight}`
  - The event type name is `{EntityName}{State}`, for example `OrderPaid`.
  - Startup validation runs before the first write. Any failure stops the generator (FR-023). Rules:
    - The initial state exists and is not terminal.
    - Every transition endpoint is a declared state.
    - Each non-terminal state has at least one outgoing transition, and terminal states have none.
    - Every state is reachable from the initial state.
    - Every state can reach a terminal state.
    - The graph has no cycles, so every lifecycle is finite.
    - Weights are > 0.
  - Next-step choice is weighted random over the allowed outgoing transitions. The `Random` instance
    is seedable (`Generator:RandomSeed`, optional) so tests are reproducible.
- **Default (Order)**:
  - Transitions:
    - Created→Paid (85)
    - Created→Cancelled (15)
    - Paid→Shipped (90)
    - Paid→Cancelled (10)
    - Shipped→Delivered (100)
  - Terminal states: Delivered, Cancelled.
- **Rationale**: This meets clarify answers Q3 and Q4. The DAG rule guarantees that the run ends and
  that every path is valid.
- **Alternatives considered**:
  - A hardcoded enum flow. Rejected because Q4 requires configurability.
  - Allowing cycles, such as Paid→Created. Rejected: unbounded event counts and no clear "valid
    path" check.

## R4. Event generation model and ordering

- **Decision**:
  - The generator keeps an in-memory **active pool** of non-terminal entities, capped by
    `Generator:MaxActiveEntities` (default 10,000).
  - For each event slot in a batch:
    - If the pool is full, advance a random active entity.
    - If the pool is empty, create a new entity.
    - Otherwise advance with probability `Generator:AdvanceProbability` (default 0.6), else create.
  - Every event gets the next global `SequenceNo` (allocated from `MAX(Outbox.SequenceNo)` at
    startup, single generator).
  - An entity has at most one event per batch, so its events always get strictly increasing
    `SequenceNo` in lifecycle order.
  - All events of an entity use the entity's partition key (Order: `CustomerId`) as `AggregateKey`
    and `Partitioning.GetPartition(key, PartitionCount)` (reused), so they land in one partition
    (FR-003, FR-024).
  - One batch is one SQL transaction. It writes the entity rows and state changes and all Outbox rows
    (FR-002).
  - **Target**: the generator counts the existing Outbox rows at startup and stops exactly when the
    total reaches `TargetEventCount`.
  - Entities left in non-terminal states are reported as "incomplete lifecycles". Their event
    sequence is still a valid prefix of a lifecycle.
  - **Restart**: the active pool is reloaded from `dbo.Orders` where the status is not terminal.
- **Rationale**: Interleaving creates realistic, mixed streams and still meets the per-partition
  order rules, because the order is global and monotonic. One event per entity per batch keeps the
  ordering argument trivial. Reloading from SQL makes restarts safe.
- **Alternatives considered**:
  - Writing each order's full lifecycle at creation time with contiguous sequence numbers. Rejected:
    it is unrealistic, and it is hard to stop exactly at the target in the middle of an order.
  - A per-partition sequence. Rejected: it needs a new allocator, and the global order already
    implies per-partition order.

## R5. Entity-definition abstraction (extensibility)

- **Decision**:
  - The pipeline (pool, sequence allocation, partitioning, Outbox insert, transaction, publisher)
    depends only on two interfaces:
    - `IFakeEntityDefinition`: `Name`, `CreateNew(Random, seq, utcNow)`, `PartitionKey(entity)`,
      `Lifecycle`, `BuildPayload(entity, transition)`.
    - `IEntityWriter`: writes or updates the entity's own table rows inside the shared
      `SqlTransaction`, and loads non-terminal entities on restart.
  - Order is the only registered production entity.
  - SC-008 is proven by a **test-only** sample entity whose definition and in-memory writer go
    through the same pipeline without changes to pipeline code.
- **Rationale**: This is FR-004 and US4: a new entity is a new definition, writer, migration and
  configuration entry, and the pipeline needs no edits.
- **Alternatives considered**:
  - A generic `EntityState(EntityType, EntityId, Status, Json)` table. Rejected: it hides the real
    `Orders` table that the monitoring scripts use.
  - Reflection or attribute-driven field generation. Rejected: too much machinery for one entity.

## R6. Schema changes and lease seeding

**Finding**:
- `dbo.Orders` has `UX_Orders_SequenceNo UNIQUE` and no status.
- `03-multinstance-publisher.sql` seeds lease rows 0..5 hardcoded.
- The worker only validates that the lease rows exist.

- **Decision**:
  - Keep `Orders.SequenceNo` as the order's **creation** event sequence. It stays unique, so no
    constraint change is needed.
  - Add the new migration `src/sql/04-lifecycle.sql`. It is idempotent, with `COL_LENGTH` and
    `INDEXPROPERTY` guards. It adds:
    - `Orders.Status NVARCHAR(30) NOT NULL DEFAULT 'Created'`
    - `Orders.LastEventSequenceNo BIGINT NULL`
    - `Orders.UpdatedAtUtc DATETIME2 NULL`
    - a filtered index on non-terminal status, for the restart reload
  - Outbox needs no changes: `EventType NVARCHAR(100)` already fits.
  - At startup the publisher **ensures** lease rows `0..PartitionCount-1` with an idempotent
    insert-if-missing, then validates them as today. This replaces the hardcoded seed dependency;
    the 03 script stays as is.
- **Rationale**: This is the smallest schema change, it runs on both a clean and an existing
  database, and the partition count comes from configuration (Constitution IV).
- **Alternatives considered**:
  - Dropping the unique index and storing a sequence per event on Orders. Rejected: the Outbox
    already holds the per-event sequence.
  - A separate `OrderStatusHistory` table. Rejected: it duplicates the Outbox.

## R7. Publisher failure path

**Finding**: On `KafkaPublishBatchException`, the worker:
1. Publishes the prefix before the failed event again, in a new transaction.
2. Marks that prefix published.
3. Calls `MarkFailedAsync` for the failed event only.
4. Waits `RetryDelay`.

The aborted first transaction is invisible to `read_committed` consumers.

- **Decision**:
  - Keep this behaviour. It is correct: the prefix is committed before it is marked, only the failed
    event is charged, and later events stay blocked.
  - Add unit tests for the failed-event mapping and the prefix path.
  - SQL errors and Kafka errors get separate catch blocks and separate log categories.
  - Kafka `ProducerFenced` or a fatal error ends the lane (the lease is released in `finally`).
- **Rationale**: Constitution III and I. Simplifying this path brings no correctness gain and carries
  regression risk.
- **Alternatives considered**: Dropping the prefix re-publish and retrying the whole batch later.
  Rejected: it costs throughput and charges no one for the failure, which would still be correct
  but changes behaviour without a need.

## R8. Run report, duplicates and throughput baseline

- **Decision**:
  - Report sections:
    - **SQL part**: `RunReportWriter` queries:
      - generated events (total and this run)
      - published
      - dead-lettered
      - pending
      - incomplete lifecycles
      - the publish window (`MIN/MAX PublishedAtUtc`), which gives events/s
    - **Stream part**: a `--verify` mode of the same executable (`StreamVerifier`) reads the topic
      from the beginning with `IsolationLevel.ReadCommitted` until the end offsets and computes:
      - duplicate `event-id` count
      - per-partition `sequence-no` ordering violations
      - per-order lifecycle validity against the configured `LifecycleDefinition`
      - events missing compared with SQL published
  - Output: a log summary plus `reports/run-{utcTimestamp}.json`.
  - **Baseline**: `reports/baseline.{Environment}.json`, one file per environment. The first full 500K run writes it.
    Later runs fail the verification (non-zero exit code) when the throughput is more than 20% below
    the baseline (SC-009).
- **Rationale**:
  - Duplicates can only be observed on the stream side, because the publisher cannot know about a
    crash after commit. So a truthful count needs a consumer (Constitution VI, FR-021).
  - Using one executable avoids a new project.
- **Alternatives considered**:
  - Counting duplicates in the publisher. Rejected: it is impossible to do truthfully.
  - A separate verifier project. Rejected: extra deployment for no benefit.

## R9. Testing approach

- **Decision**:
  - **Unit** (always run):
    - partition mapping (existing)
    - lifecycle validation (each rule)
    - weighted path validity
    - batch builder: target stop and one event per entity per batch
    - sequence order in a batch
    - retry / dead-letter decision
    - failed-event index mapping from `WhenAll`
    - TransactionalId format
    - sample entity through the pipeline (SC-008)
  - **SQL integration** (`FAKEGEN_TEST_SQL`):
    - existing concurrency test
    - expired-lease takeover
    - token increment
    - stale renew / publish / fail / release
    - generator transaction atomicity
    - lease ensure is idempotent
  - **Kafka integration** (`FAKEGEN_TEST_KAFKA` = bootstrap servers, creates a temporary topic with
    6 partitions):
    - batch commit
    - abort not visible to `read_committed`
    - fencing of a zombie producer with the same TransactionalId
    - ordering
    - parallel partitions
  - **E2E** (both variables):
    - migrations on a clean database
    - 500K events
    - two publisher instances
    - kill one
    - takeover within one lease period
    - `--verify` reports 0 lost, 0 ordering violations, 0 lifecycle violations, and duplicates only
      if a crash happened
  - Tests skipped because a variable is missing are reported as **not run**.
- **Rationale**: This covers every group in docs/spec.md §11, and it reuses the existing opt-in
  pattern and the team's shared Kafka/SQL environment.
- **Alternatives considered**: Testcontainers. Rejected: it adds a Docker dependency, and a real
  multi-broker cluster already exists. It can be revisited later.

## R10. Leftovers found during reading

- **`src/Models/OrderEvent.cs`**:
  - It declares an unused class wrongly named `Partitioning`.
  - **Decision**: after a usage review, replace it with the lifecycle payload model `OrderEventPayload`
    (see data-model.md).
- **`src/sql/SQLQuery1.sql` and `SQLQuery2.sql`**:
  - These are ad hoc scripts.
  - **Decision**: leave them untouched. They are outside the feature scope.
- **Changing `Kafka:PartitionCount` after events exist**:
  - This would remap keys and break per-key ordering across runs.
  - **Decision**: document it as an operational constraint in the README and quickstart. Automatic
    repartitioning stays out of scope.

## v2 research decisions — 2026-11-22

This section supersedes v1 delivery/baseline assumptions above. Detailed normative
choices are in contracts/v2-processing.md. Research performed directly in this
session; no subagents were requested or launched.

- R10 Decision: Kafka Streams EOS v2, Java 17 baseline, Streams 4.1.2 design target,
  classic consumer-group protocol. Rationale: transactional state/output/offsets with
  documented EOS-v2 upgrade support. Alternative: custom .NET receipts requires an
  additional recovery proof. Deployment remains gated on actual broker API and topic-ID
  compatibility; unsupported environments fail preflight, not silent client downgrade.
  Pin the build and verify the integration profile during T064; no broker upgrade here.
- R11 Decision: immutable SQL event catalog read by finalizer before forwarding.
  Rationale: local dedup alone cannot detect a copied ID under another key/partition.
  Alternative: repartition-by-ID changes original partition ordering. Cost: SQL read
  availability and throughput; batch immutable lookups, never mark delivery from Java.
- R12 Decision: SQL registry bound to cluster and topic UUIDs, restrictive runtime ACLs,
  precreated internal topics, restore/output audit before processing each assigned task.
  Rationale: application.id changes otherwise create fresh state against old output.
  Alternative: name checks alone miss recreated topics. Cost: recovery scans can miss
  18 seconds; the acceptance gate remains binding and requires measurement/redesign if failed.
- R13 Decision: decimal-json-v1 typed canonical grammar with exact decimal tokens.
  Rationale: binary-double canonicalization cannot preserve all monetary/large decimal
  values. Alternative: raw bytes misclassify object-order/numeric-format equivalents.
  Cross-language fixtures are mandatory before either implementation.
- R14 Decision: global pipeline audit starts, explicit selected RunId, classify known
  other runs separately. Rationale: retained prior runs must not produce false extras;
  unknown/malformed records still invalidate the audit. Cost: full-history scanning.
- R15 Decision: atomic run progress/closure and explicit new-run operation, fresh entity
  pool per run. Rationale: restart must not invent a new run or reuse the database-wide
  target count. Legacy data gets explicit registration, never silent reassignment.
- R16 Decision: v2-only final-observer performance baselines and deterministic recovery
  measurement; ingress SQL timing is a separate metric. Old baselines remain historical.

Primary sources checked in planning:
https://kafka.apache.org/41/streams/upgrade-guide/ — EOS-v2 and upgrade compatibility.
https://kafka.apache.org/documentation/streams/developer-guide/running-app/ — restore lifecycle.
https://kafka.apache.org/39/streams/developer-guide/config-streams/ — application configuration.
https://kafka.apache.org/design/ — transaction/read_committed boundaries.
These sources support the Kafka primitives; catalog/registry/canonicalization decisions
are project-specific design choices, not Kafka-provided guarantees.

### R17 — U7 assignment-scoped audit ordering
Decision: bind audit to a cancellable task-assignment token and enforce the ordered
readiness/boundary/gate checks in contracts/v2-processing.md through supported Streams
lifecycle integration. Rationale: restore completion alone does not prove old-owner
transactions and final output are a stable comparison snapshot. Alternative rejected:
using RUNNING as a fence or creating another producer with Streams transactional IDs.
The local token guards stale callbacks; Kafka retains transactional fencing authority.
Lifecycle feasibility is an explicit T096 test gate, not an assumed API capability.
Sources: https://kafka.apache.org/41/streams/developer-guide/running-app/ (restore before
processing); https://kafka.apache.org/41/streams/architecture/ (task migration/state);
https://kafka.apache.org/41/configuration/kafka-streams-configs/ (EOS-v2 settings).
The audit protocol is a project design on top of these primitives, not a Kafka API promise.

### R18 — U8 drain evidence
Decision: quiesce ingress with a persistent epoch barrier and use broker-committed
finalizer offsets through the captured committed-business input cut before final audit.
Rationale: SQL marks prove only ingress acknowledgement; local consumer positions may
advance before transactions commit. Transaction markers can make raw high-watermark
comparison unsuitable for the group offset. Alternatives rejected: pending=0, fixed
sleep or local lag as success. Cost: explicit pipeline-wide audit pause and coordinator
recovery operation. Failure remains incomplete, not a relaxed acceptance criterion.

### R19 — U9 supported error handling limitation
Decision: do not select a handler-only retry strategy; keep isolation as an explicit
feasibility gate. Kafka 4.1 processing.exception.handler has FAIL/CONTINUE; production
RETRY applies to retriable production errors. Neither proves independent SQL/catalog
retry under shared tasks/transactions. Alternatives requiring deployment changes are
not yet approved. The candidate TaskAssignor API needs assignment-granularity proof.
Sources checked 2026-11-23:
https://kafka.apache.org/41/streams/developer-guide/config-streams/
https://kafka.apache.org/41/configuration/kafka-streams-configs/
The incompatibility with this project's local no-loss retry requirement is our design
assessment; no runtime prototype or test was executed in this documentation stage.
