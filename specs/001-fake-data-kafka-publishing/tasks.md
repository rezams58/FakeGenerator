---

description: "Task list for Fake Data Generation and Ordered Event Publishing"
---

# Tasks: Fake Data Generation and Ordered Event Publishing

**Input**: Design documents from `/specs/001-fake-data-kafka-publishing/`

**Prerequisites**: plan.md, spec.md, research.md (R1–R10), data-model.md, contracts/, quickstart.md

**Tests**: INCLUDED. Constitution VI and docs/spec.md §11 require unit, SQL, Kafka and E2E tests that are actually executed. Tests that cannot run (missing `FAKEGEN_TEST_SQL` / `FAKEGEN_TEST_KAFKA`) MUST be reported as "not run".

**Organization**: Grouped by user story. Paths are relative to the repository root.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependency on an incomplete task)
- **[Story]**: US1–US4, only in user-story phases

---

## Phase 1: Setup

- [X] T001 Create folders `src/Generation/`, `src/Generation/Order/`, `src/Reporting/`, `tests/FakeGenerator.Tests/Unit/`, `tests/FakeGenerator.Tests/Sql/`, `tests/FakeGenerator.Tests/Kafka/` (add `.gitkeep` only where still empty)
- [X] T002 [P] Add `reports/` to `.gitignore` (create the file if missing) so run reports and baselines are not committed
- [X] T003 [P] Create test helper `tests/FakeGenerator.Tests/TestEnv.cs` exposing `FAKEGEN_TEST_SQL` and `FAKEGEN_TEST_KAFKA` and a skip helper that marks a test as "not run" with a reason when the variable is missing
- [X] T004 Move `tests/FakeGenerator.Tests/PartitioningTests.cs` to `tests/FakeGenerator.Tests/Unit/` and `tests/FakeGenerator.Tests/SqlLeaseConcurrencyTests.cs` to `tests/FakeGenerator.Tests/Sql/` (namespace updates only, no behaviour change)
- [X] T005 Run `dotnet build FakeGenerator.sln -c Release` and `dotnet test tests/FakeGenerator.Tests -c Release` once to record the real baseline build/test state in the report (IMPLEMENTATION_NOTES.md says they were never run)

---

## Phase 2: Foundational (blocks all user stories)

- [X] T006 Fix the constructor of `KafkaPublisherWorker` in `src/Workers/KafkaPublisherWorker.cs`: assign `_enabled`, `_batchSize`, `_idleDelay`, `_retryDelay`, `_maxRetryCount`, `_leaseSeconds`, `_renewSeconds`, `_maxPartitions` from `PublisherConfig` (research R1)
- [X] T007 Search usages of the `Equals`/`GetHashCode` overrides in `src/Workers/KafkaPublisherWorker.cs`; if unused, remove them (Constitution: no deletion without usage review)
- [X] T008 Add `TargetEventCount` (default 500000), `IntervalMilliseconds` (default 100, rule `≥ 0`), `MaxActiveEntities` (10000), `AdvanceProbability` (0.6), `RandomSeed` (null) to `GeneratorConfig` and keep `RecordCount` as an obsolete alias that logs a warning in `src/Configuration/GeneratorSettings.cs` (contracts/configuration.md)
- [X] T009 [P] Add `ReportingConfig` (`Directory` default `reports`, `Environment` default machine name, `RegressionThresholdPercent` default 20) in `src/Configuration/GeneratorSettings.cs`
- [X] T010 Add options validators with `ValidateOnStart` in `src/Configuration/OptionsValidators.cs` and register them in `src/Program.cs`. T010 implements every rule of `contracts/configuration.md`: Kafka `BootstrapServers`, `Topic`, `ClientId` non-empty, `PartitionCount ≥ 6`, `Acks` must be `All`, `CompressionType` must parse, `LingerMs ≥ 0`, producer `BatchSize > 0`; Generator `TargetEventCount > 0`, `BatchSize > 0`, `IntervalMilliseconds ≥ 0`, `MaxActiveEntities > 0`, `0 < MinOrderAmount ≤ MaxOrderAmount`, `CustomerCount > 0`, `OrderHistoryDays ≥ 0`, `0 < AdvanceProbability < 1`; Publisher `BatchSize > 0`, `IdleDelayMilliseconds ≥ 0`, `RetryDelayMilliseconds ≥ 0`, `MaxRetryCount ≥ 1`, `MaxPartitionsPerInstance ≥ 1`, `0 < LeaseRenewIntervalSeconds < LeaseDurationSeconds`; Reporting `RegressionThresholdPercent > 0`, `Directory` writable. Lifecycle graph rules are validated by `LifecycleDefinition` (T016, T021)
- [X] T011 [P] Unit tests for every validator rule (valid and invalid case each) in `tests/FakeGenerator.Tests/Unit/OptionsValidatorTests.cs`
- [X] T012 Create idempotent migration `src/sql/04-lifecycle.sql` adding to `dbo.Orders`: `Status NVARCHAR(30) NOT NULL DEFAULT 'Created'`, `LastEventSequenceNo BIGINT NULL`, `UpdatedAtUtc DATETIME2(3) NULL`, plus a filtered index on `Status` for non-terminal states; guard with `COL_LENGTH` / `sys.indexes` checks
- [X] T013 Add `EnsureLeasePartitionsAsync(int partitionCount)` (idempotent insert-if-missing for `PartitionId 0..partitionCount-1`) to `src/Data/SqlOrderRepository.cs` and call it before `ValidateLeasePartitionsAsync` in `src/Workers/KafkaPublisherWorker.cs` (research R6)
- [X] T014 [P] SQL integration test: running `04-lifecycle.sql` and `EnsureLeasePartitionsAsync` twice on a clean DB succeeds with no change, in `tests/FakeGenerator.Tests/Sql/MigrationIdempotencyTests.cs` (needs `FAKEGEN_TEST_SQL`)
- [X] T015 Introduce testable seams without behaviour change: `IPartitionPublisher` (`PublishBatchAsync`, `Dispose`) implemented by `PartitionedKafkaPublisher` in `src/Kafka/IPartitionPublisher.cs`, a publisher factory, and an `IOutboxRepository` interface over the lease/outbox methods of `src/Data/SqlOrderRepository.cs`; inject them into `src/Workers/KafkaPublisherWorker.cs` and register them in `src/Program.cs`

**Checkpoint**: publisher is enabled and unit-testable, configuration fails fast, schema ready.

---

## Phase 3: User Story 1 — Generate fake orders at volume (P1)

**Goal**: Generate orders with lifecycle events until exactly 500,000 events exist, each order following a valid path (FR-001–FR-004, FR-023, FR-024, SC-001, SC-010).

**Independent Test**: Run only the generator against a clean DB; Outbox count equals `TargetEventCount`, every order's events form a valid path, a restart reuses no sequence number, and an invalid lifecycle config leaves the DB untouched.

### Tests for US1

- [X] T016 [P] [US1] Unit tests for lifecycle validation in `tests/FakeGenerator.Tests/Unit/LifecycleDefinitionTests.cs`: initial state exists and is not terminal; endpoints declared; non-terminal states have an outgoing transition; terminal states have none; all states reachable from the initial state; every state reaches a terminal state; no cycles; `Weight > 0`
- [X] T017 [P] [US1] Unit test in `tests/FakeGenerator.Tests/Unit/LifecyclePathTests.cs`: with a fixed `RandomSeed`, 10,000 generated paths are all valid, Cancelled appears only from Created or Paid
- [X] T018 [P] [US1] Unit tests for `EventBatchBuilder` in `tests/FakeGenerator.Tests/Unit/EventBatchBuilderTests.cs`: stops exactly at target (also mid-lifecycle), at most one event per entity per batch, `SequenceNo` strictly increasing, same `AggregateKey` gives same `KafkaPartition`, per-order events in lifecycle order
- [X] T019 [P] [US1] SQL integration test in `tests/FakeGenerator.Tests/Sql/GeneratorAtomicityTests.cs`: entity row change and its Outbox row commit together, and a forced failure rolls both back (needs `FAKEGEN_TEST_SQL`)
- [X] T020 [P] [US1] SQL integration test in `tests/FakeGenerator.Tests/Sql/GeneratorRestartTests.cs`: stop the generator mid-run, restart it; no `SequenceNo` reused, no duplicate order or event, active non-terminal orders continue their lifecycle, final Outbox count still equals `TargetEventCount` (US1 scenario 3; needs `FAKEGEN_TEST_SQL`)

### Implementation for US1

- [X] T021 [P] [US1] Create `LifecycleDefinition` (InitialState, States, TerminalStates, Transitions `{From, To, Weight}`, graph validation, seedable weighted next-step) in `src/Generation/LifecycleDefinition.cs`
- [X] T022 [P] [US1] Create `IFakeEntityDefinition` (`Name`, `CreateNew`, `PartitionKey`, `Lifecycle`, `BuildPayload`, plus the list of field names usable as partition key) in `src/Generation/IFakeEntityDefinition.cs`
- [X] T023 [P] [US1] Create `IEntityWriter` (`InsertNewAsync`, `ApplyTransitionAsync`, `LoadActiveAsync`) in `src/Generation/IEntityWriter.cs`
- [X] T024 [US1] Create `OrderDefinition` in `src/Generation/Order/OrderDefinition.cs`: `OrderNumber` as `ORD-{SequenceNo:D10}`, `CustomerId` as `CUS-{1..CustomerCount:D6}` (partition key), `Amount` within `MinOrderAmount`..`MaxOrderAmount`, `CreatedAtUtc` within `OrderHistoryDays`, default lifecycle from configuration
- [X] T025 [US1] Create lifecycle payload model `OrderEventPayload` (fields per contracts/kafka-message.md: `OrderId`, `SequenceNo`, `OrderNumber`, `CustomerId`, `Amount`, `CreatedAtUtc`, `EventType`, `PreviousStatus`, `Status`, `OccurredAtUtc`, `EventVersion`) in `src/Models/OrderEventPayload.cs`; after a usage search, delete the unused misnamed `src/Models/OrderEvent.cs`
- [X] T026 [US1] Create `OrderWriter` in `src/Generation/Order/OrderWriter.cs`: insert and update `dbo.Orders` (`Status`, `LastEventSequenceNo`, `UpdatedAtUtc`) inside the shared `SqlTransaction`; `LoadActiveAsync` reads non-terminal orders
- [X] T027 [US1] Create `EventBatchBuilder` in `src/Generation/EventBatchBuilder.cs`: active pool capped by `MaxActiveEntities`, `AdvanceProbability`, one event per entity per batch, global `SequenceNo` from the current max, partition via `Partitioning.GetPartition` in `src/Data/Partitioning.cs`, stop at `TargetEventCount` (research R4)
- [X] T028 [US1] Add a batch write method to `src/Data/SqlOrderRepository.cs` that writes entity changes and Outbox rows (`EventType`, `AggregateKey`, `KafkaPartition`, `Payload`) in one transaction using the existing OPENJSON insert; add `CountOutboxAsync` for the target check
- [X] T029 [US1] Create the generic generator worker `src/Workers/GeneratorWorker.cs` over `IFakeEntityDefinition` (after a usage search, delete `src/Workers/OrderGeneratorWorker.cs`): validate lifecycle config before any write (refuse to start on failure), reload the active pool on restart, count existing Outbox rows toward the target, wait `IntervalMilliseconds` between batches, and at stop log orders created, events generated this run and in total, and incomplete lifecycles
- [X] T030 [US1] Register definitions, writer and builder in `src/Program.cs`; add startup validation that `Generator:Entities:Order:PartitionKey` names a field the Order definition exposes; add `Generator:Entities:Order` (PartitionKey `CustomerId`, default Lifecycle), `TargetEventCount` and `IntervalMilliseconds` to `src/appsettings.json`
- [X] T031 [US1] Run the US1 unit tests and, if `FAKEGEN_TEST_SQL` is set, the SQL tests; record real output

**Checkpoint**: US1 works alone.

---

## Phase 4: User Story 2 — Publish with per-partition ordering (P1)

**Goal**: Publish events per partition in sequence order, with retry, dead letter and fail-fast topology checks (FR-005–FR-012, FR-019, FR-020, SC-002, SC-006).

**Independent Test**: With pending events in the Outbox, start one publisher; every event ends published or dead-lettered, each partition is in ascending order, nothing is marked published unless its Kafka transaction committed, and a topic with fewer than 6 partitions stops the worker.

### Tests for US2

- [X] T032 [P] [US2] Unit tests in `tests/FakeGenerator.Tests/Unit/PublisherUnitTests.cs`: TransactionalId format `{ClientId}-{Topic}-partition-{id}`, in-batch sort by `SequenceNo`, retry versus dead-letter decision at `MaxRetryCount`
- [X] T033 [P] [US2] Unit test for failed-event detection across concurrent deliveries (first faulted index is the one charged, earlier ones are not) in `tests/FakeGenerator.Tests/Unit/FailedEventMappingTests.cs`; extract the mapping into a testable method in `src/Kafka/PartitionedKafkaPublisher.cs` if needed
- [X] T034 [P] [US2] Unit tests with fake `IPartitionPublisher` / `IOutboxRepository` in `tests/FakeGenerator.Tests/Unit/PublisherWorkerTests.cs`: commit throws → `MarkPublishedAsync` never called; batch abort on a delivery failure → only the failed event gets `MarkFailedAsync`, earlier events are not charged; success → exactly one bulk `MarkPublishedAsync` after commit (docs/spec.md §11 "Abort and Commit", §12 "SQL update only after Kafka commit")
- [X] T035 [P] [US2] Kafka integration tests in `tests/FakeGenerator.Tests/Kafka/PublisherKafkaTests.cs` using a temporary 6-partition topic (needs `FAKEGEN_TEST_KAFKA`): batch commit, abort invisible to `read_committed`, ordering within a partition, partitions progress in parallel
- [X] T036 [P] [US2] Kafka tests in `tests/FakeGenerator.Tests/Kafka/TopicValidationTests.cs`: a topic with fewer than 6 partitions, a missing topic, and an unreachable broker (metadata error) each make `KafkaTopicValidator` fail
- [X] T037 [P] [US2] SQL test that published and dead-lettered events are never selected by `ReadPendingAsync`, that a failed event blocks later ones of the same partition, and that after an event is dead-lettered the next event of the same partition is selected (FR-010), in `tests/FakeGenerator.Tests/Sql/OutboxSelectionTests.cs`

### Implementation for US2

- [X] T038 [US2] In `src/Workers/KafkaPublisherWorker.cs` handle SQL errors and Kafka errors in separate catch blocks with separate log messages; keep the prefix re-publish and commit-before-mark behaviour (research R7); confirm with T034
- [X] T039 [US2] Ensure `KafkaPublisherWorker` stops (does not continue) when `KafkaTopicValidator.Validate` throws; confirm with T036
- [X] T040 [US2] Persist enough error detail in `LastError` (truncated to 2000 chars) for the failed event only in `src/Data/SqlOrderRepository.cs` (`MarkFailedAsync`); verify with T037
- [X] T041 [US2] Run US2 tests and a local publish of the US1 output; record real output and list Kafka tests as "not run" if `FAKEGEN_TEST_KAFKA` is missing

**Checkpoint**: US1 and US2 work together (MVP).

---

## Phase 5: User Story 3 — Several publisher instances (P2)

**Goal**: Exclusive, fenced partition ownership with takeover after a crash (FR-013–FR-018, SC-003, SC-005).

**Independent Test**: Two instances share the partitions; killing one lets the other claim its partitions within one lease duration plus one renewal interval, and a stale owner fails to renew, publish or mark.

### Tests for US3

- [X] T042 [P] [US3] SQL tests in `tests/FakeGenerator.Tests/Sql/LeaseLifecycleTests.cs`: concurrent claim gives one owner; expired-lease takeover; `FencingToken` increments on every transfer; stale renew/release/read/mark-published/mark-failed all fail; a release by the old owner does not free the new ownership; with all 6 partitions owned, an extra instance claims nothing and raises no error (`MaxPartitionsPerInstance` is an upper bound only)
- [X] T043 [P] [US3] Unit test in `tests/FakeGenerator.Tests/Unit/PublisherWorkerTests.cs`: when `RenewLeaseAsync` fails (stale token), the lane stops before the next `PublishBatchAsync` and sends nothing more for that partition (FR-016, Constitution II)
- [X] T044 [P] [US3] Kafka fencing test in `tests/FakeGenerator.Tests/Kafka/FencingTests.cs`: a second producer with the same TransactionalId fences the first, and the zombie cannot commit
- [X] T045 [US3] E2E test in `tests/FakeGenerator.Tests/Kafka/TwoInstanceE2ETests.cs` (needs both variables): two publisher instances, kill one, takeover within one lease duration plus one renewal interval (SC-003), no event lost

### Implementation for US3

- [X] T046 [US3] If T042–T045 expose gaps, fix them in `src/Data/SqlOrderRepository.cs` or `src/Workers/KafkaPublisherWorker.cs` (renew failure must stop sends for that partition immediately, per T043); otherwise record "no change needed" with the test output

---

## Phase 6: User Story 4 — Add new fake entity types (P3)

**Goal**: A new entity is added by definition, writer, migration and config only, with no change to the pipeline (FR-004, SC-008).

**Independent Test**: A test-only sample entity runs through `EventBatchBuilder` with a different partition key and lifecycle, and no file under `src/Generation/` (except the new entity's own folder) or `src/Workers/` is touched.

- [X] T047 [P] [US4] Create a test-only `SampleEntityDefinition` and in-memory `SampleEntityWriter` (different partition key, 3-state lifecycle) in `tests/FakeGenerator.Tests/Unit/SampleEntity/`
- [X] T048 [US4] Unit test `tests/FakeGenerator.Tests/Unit/NewEntityPipelineTests.cs`: the sample entity produces valid paths, uses its own partition key for `AggregateKey`/`KafkaPartition`, and stops at the target
- [X] T049 [US4] Add an "Adding a new entity" section to `src/README.md` listing the four steps (definition, writer, migration, config entry)

---

## Phase 7: Polish & Cross-Cutting

- [X] T050 [P] Create `RunReport` model per data-model.md §6 (including `ordersGenerated`) in `src/Reporting/RunReport.cs`
- [X] T051 Create `RunReportWriter` in `src/Reporting/RunReportWriter.cs`: SQL counts (orders generated, events generated, published, dead-lettered, pending, incomplete lifecycles), publish window and events/s, write `reports/run-{utc}.json`, compare with `reports/baseline.{Environment}.json` (create it on the first full run, fail when more than `RegressionThresholdPercent` slower)
- [X] T052 Create `StreamVerifier` in `src/Reporting/StreamVerifier.cs`: `read_committed` consumer from the beginning to the end offsets, count duplicate `event-id`, per-partition `sequence-no` violations, per-order lifecycle violations, dead-letter gaps (using the SQL list of dead-lettered events), missing events versus SQL published (contracts/consumer-inbox.md)
- [X] T053 Add the `--verify` mode to `src/Program.cs`: it is the run-report trigger, executed after a generate + publish run; it runs `StreamVerifier` and `RunReportWriter`, writes the report, and exits non-zero on any violation or regression
- [X] T054 [P] Create a reference Inbox consumer in `tests/FakeGenerator.Tests/Kafka/InboxConsumer.cs` per contracts/consumer-inbox.md: `IsolationLevel.ReadCommitted`, `EnableAutoCommit = false`, `EventId` from the `event-id` header, `dbo.Inbox` with PK on `EventId`, Inbox insert and a business effect (e.g. a counter row) in one SQL transaction, PK violation 2627/2601 → skip, offset commit after SQL commit
- [X] T055 E2E consumer deduplication test in `tests/FakeGenerator.Tests/Kafka/ConsumerDeduplicationE2ETests.cs` (needs both variables): force a crash between Kafka commit and SQL mark (e.g. a test `IOutboxRepository` that throws on `MarkPublishedAsync` once, then a fresh lane), confirm the events are re-published with the same `event-id`, and confirm the `InboxConsumer` applies each business effect exactly once (SC-004, docs/spec.md §11 E2E #10, §12)
- [X] T056 [P] Unit tests for baseline comparison (first run creates the baseline, 19% slower passes, 21% slower fails) in `tests/FakeGenerator.Tests/Unit/BaselineTests.cs`
- [X] T057 [P] Unit tests for lifecycle and ordering checks of the verifier core logic (valid path, bad path, gap from dead letter counted as gap not violation, duplicate) in `tests/FakeGenerator.Tests/Unit/StreamVerifierLogicTests.cs`
- [X] T058 [P] Update `src/README.md`: new settings, lifecycle config, `--verify`, at-least-once wording, consumer Inbox contract, constraint that `Kafka:PartitionCount` must not change after events exist, that only one generator instance may run at a time, and that all instances must share bootstrap servers, topic, partition count and `ClientId` (different values are unsupported)
- [X] T059 [P] Update `IMPLEMENTATION_NOTES.md` with real build/test results and the list of tests not run
- [X] T060 Run the full E2E from `specs/001-fake-data-kafka-publishing/quickstart.md` (500K events, two instances, kill one, `--verify`); record real counts
- [X] T061 Verify the docs/spec.md §12 acceptance criteria one by one against real output
- [X] T062 Remove the Sync Impact Report HTML comment at the top of `.specify/memory/constitution.md` before the commit
- [X] T063 Produce the final report (changed files, architecture, real build/test output, tests not run, residual risks), then run `/code-review 62bcc36`

---

## Dependencies & Execution Order

- Phase 1 → Phase 2 → Phase 3 (US1) → Phase 4 (US2) → Phase 5 (US3).
- Phase 6 (US4) depends only on Phase 3.
- Phase 7: T050–T057 need Phase 3 and Phase 4 (T055 also needs T015 and T054); T060–T063 come last.
- Inside a story: tests first (they must fail first) → interfaces and models → writer/builder → worker → wiring.
- T006 (publisher fix) and T015 (seams) block every publishing test.

## Parallel Examples

- US1 tests: T016, T017, T018, T019, T020 together.
- US1 interfaces: T021, T022, T023 together.
- US2 tests: T032–T037 together.
- US3 tests: T042, T043, T044 together.
- Polish: T054, T056, T057, T058, T059 together.

## Implementation Strategy

- **MVP**: Phase 1–2, US1, US2. This gives a publisher that runs and a generator that produces valid lifecycles.
- **Increment**: US3 (multi-instance proof), US4 (extensibility), then Polish (report, verifier, consumer dedup, E2E).
- Stop after each checkpoint and run the tests for real. Do not claim success without the output.

## Requirement Coverage

| Requirement | Tasks |
|-------------|-------|
| FR-001–FR-004, FR-023, FR-024 | T008, T016–T030 |
| FR-005–FR-012 | T032–T041 |
| FR-013–FR-018 | T042–T046 (FR-016: T043) |
| FR-019, FR-020 | T035, T052, T054, T055, T058 |
| FR-021 | T050–T053 |
| FR-022 | T008–T011, T030 |
| SC-001, SC-010 | T018, T020, T029, T052, T057, T060 |
| SC-002, SC-006 | T035, T036, T052 |
| SC-003, SC-005 | T042–T045 |
| SC-004 | T054, T055 |
| SC-007, SC-009 | T051, T053, T056 |
| SC-008 | T047, T048 |
