# Data Model: Fake Data Generation and Ordered Event Publishing

Phase 1 output for [plan.md](./plan.md). The design decisions behind this model are in
[research.md](./research.md) (R3–R6, R8).

## 1. Order — `dbo.Orders` (existing + `04-lifecycle.sql`)

| Field | Type | Rule |
|-------|------|------|
| OrderId | UNIQUEIDENTIFIER PK | New GUID per order |
| SequenceNo | BIGINT, UNIQUE (`UX_Orders_SequenceNo`) | `SequenceNo` of the order's **creation** event (unchanged meaning) |
| OrderNumber | NVARCHAR(40) | `ORD-{SequenceNo:D10}` |
| CustomerId | NVARCHAR(40) | `CUS-{1..CustomerCount:D6}`. This is the **partition key** |
| Amount | DECIMAL(18,2) | Uniform in [`MinOrderAmount`, `MaxOrderAmount`] |
| CreatedAtUtc | DATETIME2(3) | Within the last `OrderHistoryDays` days |
| **Status** (new) | NVARCHAR(30) NOT NULL, default `'Created'` | Current lifecycle state; must be a declared state |
| **LastEventSequenceNo** (new) | BIGINT NULL | `SequenceNo` of the latest event; strictly increasing per order |
| **UpdatedAtUtc** (new) | DATETIME2(3) NULL | Time of the last transition |

New index: a filtered index on `(Status)` that covers only the non-terminal states. The restart path
uses it to reload the active pool.

## 2. Pending Event — `dbo.Outbox` (schema unchanged)

| Field | Rule |
|-------|------|
| EventId | GUID, unique. It becomes the Kafka `event-id` header and the consumer dedup key |
| SequenceNo | Global, strictly increasing, UNIQUE. It is allocated by the single generator |
| AggregateKey | The entity's partition key value (Order: `CustomerId`) |
| KafkaPartition | `Partitioning.GetPartition(AggregateKey, Kafka:PartitionCount)` (FNV-1a) |
| EventType | `{Entity}{State}`, for example `OrderCreated`, `OrderPaid`, `OrderShipped`, `OrderDelivered`, `OrderCancelled` |
| Payload | JSON. See [contracts/kafka-message.md](./contracts/kafka-message.md) |
| CreatedAtUtc / PublishedAtUtc / DeadLetterAtUtc / RetryCount / LastError | Same meaning as today |

**Event states** (derived from the columns):

```text
Pending ──(Kafka commit + bulk mark)──► Published
   │
   └─(delivery failure: RetryCount+1, LastError)──► Pending
                       └─(RetryCount+1 ≥ MaxRetryCount)──► DeadLettered
```

Published and DeadLettered are final. A final event is never selected again (FR-011).

**Invariants**:
- All events of one order have the same `AggregateKey` and therefore the same `KafkaPartition`.
- Within one order, `SequenceNo` increases in lifecycle order.
- The Order row change and its event row are written in the same SQL transaction.

## 3. Partition Ownership — `dbo.PublisherPartitionLease` (unchanged)

`PartitionId` (PK), `OwnerId`, `LeaseUntilUtc`, `FencingToken`, `UpdatedAtUtc`.

- At startup, rows `0..Kafka:PartitionCount-1` are ensured with an idempotent insert-if-missing.
- A claim is allowed only when `LeaseUntilUtc <= SYSUTCDATETIME()`, and it sets
  `FencingToken = FencingToken + 1`.
- Renew and the outbox operations require a matching `OwnerId` and `FencingToken` and an unexpired
  lease.
- Release requires only the matching owner and token.

## 4. Lifecycle Definition (configuration, in memory)

| Field | Rule |
|-------|------|
| InitialState | Declared and not terminal |
| States | Unique names. Each name produces `EventType = {Entity}{State}` |
| TerminalStates | A subset of States, with no outgoing transitions |
| Transitions[] `{From, To, Weight}` | Endpoints must be declared states; Weight > 0 |

Graph validation runs at startup. If any rule fails, the generator refuses to start and writes
nothing:
- Every state is reachable from the initial state.
- Every state can reach a terminal state.
- The graph has no cycles.
- Every non-terminal state has at least one outgoing transition.

Default for Order:

```text
Created ─85─► Paid ─90─► Shipped ─100─► Delivered (terminal)
   │            │
   15           10
   ▼            ▼
Cancelled (terminal) ◄──┘
```

A **valid path** (SC-010) is a sequence that starts at InitialState and in which each step is a
declared transition. A path that ends in a non-terminal state is valid but **incomplete**. The run
report counts incomplete paths.

## 5. Fake Entity Definition (code contract)

`IFakeEntityDefinition` has these members:
- `Name`
- `CreateNew(Random, sequenceNo, utcNow)`
- `PartitionKey(entity)`
- `Lifecycle`
- `BuildPayload(entity, fromState, toState)`

`IEntityWriter` has these members:
- `InsertNewAsync` (inside the shared transaction)
- `ApplyTransitionAsync` (inside the shared transaction)
- `LoadActiveAsync` (for restart)

Relationships:
- One definition is linked to one writer and one lifecycle.
- One entity instance produces 1..n events.

## 6. Run Report (output, `reports/run-{utc}.json`)

| Field | Source |
|-------|--------|
| runId, startedAtUtc, finishedAtUtc, environment | Process |
| targetEventCount, generatedTotal, generatedThisRun | Config / `COUNT(*) Outbox` |
| ordersGenerated | `COUNT(*) Orders` |
| published, deadLettered, pending | Outbox columns |
| incompleteLifecycles | Orders whose Status is not terminal |
| publishWindowSeconds, eventsPerSecond | `MIN/MAX(PublishedAtUtc)` |
| duplicates, orderingViolations, lifecycleViolations, deadLetterGaps, missingOnStream | `--verify` (read_committed consumer + the SQL list of dead-lettered events) |
| baselineEventsPerSecond, regressionPercent, passed | `reports/baseline.{Environment}.json`; the run fails if it is more than 20% slower |

Rules:
- The report is produced by the `--verify` run after a generate + publish run. Long-running publishers
  do not write it themselves. The generator logs its own counts when it stops.
- Every count comes from a real query or from a real consumer.
- When the verifier was not run, its fields are `null` and the report marks them **not run**.
