# Feature Specification: Fake Data Generation and Ordered Event Publishing

**Feature Branch**: `001-fake-data-kafka-publishing`

**Created**: 2026-11-15

**Status**: Draft

**Input**: User description: "هدف پروژه تولید دیتای فیک؛ این دیتا می‌تواند شامل هر جدولی که نیاز است باشد و در این مرحله سفارش در شروع مد نظر است. در ادامه، تولید دیتای فیک، انتشار آن در کافکا و مدیریت ترتیب در پارتیشن‌ها هم جزء اصلی این نیاز می‌باشد."

## Clarifications

### Session 2026-11-15

- Q: Which key decides the partition (and therefore the ordering scope) of events? → A: Configurable per
  entity type; for Order the default key is the customer.
- Q: What minimum throughput is acceptable for publishing 500,000 messages? → A: No fixed minimum; every
  run reports throughput, the first full run is the baseline, and a drop of more than 20% from it fails.
- Q: Which events are generated and published for each fake order? → A: The order lifecycle (creation
  and later status changes), not only creation; this change to the message contract is approved.
- Q: Which states make up the order lifecycle? → A: Configurable state machine; default Created → Paid →
  Shipped → Delivered, with Cancelled allowed only before Shipped.
- Q: What does the 500,000 target count? → A: Total published events (messages), not orders.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Generate fake orders at volume (Priority: P1)

An operator (developer, tester or load-test engineer) starts the generator and asks for a given number
of events. The generator creates realistic fake orders and moves each one through a valid path of the
configured lifecycle (for example Created → Paid → Shipped → Delivered, or Created → Cancelled). Every
order change is stored together with a pending event that records it must be published. Each event is
assigned to a partition from the entity's partition key (for orders: the customer) and gets a
per-partition sequence number that fixes its publishing order.

**Why this priority**: without realistic data in volume there is nothing to publish or test; it is the
foundation of every other story.

**Independent Test**: run the generator for a fixed event count (for example 10,000) and check that
exactly that many pending events exist, each with a partition and a unique, increasing sequence number per
partition, and that every order's events follow a valid lifecycle path.

**Acceptance Scenarios**:

1. **Given** an empty data store, **When** the operator requests 500,000 events, **Then** 500,000 pending
   events (across all orders and lifecycle steps) are stored and the run reports the generated counts of
   orders and events.
2. **Given** generated events, **When** they are grouped by partition, **Then** sequence numbers inside
   each partition are unique and strictly increasing in creation order.
3. **Given** the generator is stopped mid-run and restarted, **When** generation resumes, **Then** no
   sequence number is reused and no order or event is duplicated.
4. **Given** the default lifecycle, **When** an order's events are listed in sequence order, **Then**
   they form a valid path (for example Created → Paid → Shipped → Delivered) and Cancelled never follows
   Shipped.

---

### User Story 2 - Publish generated orders with per-partition ordering (Priority: P1)

The publisher reads pending events and publishes them to the event stream. Within each partition,
events reach consumers in sequence-number order. Partitions progress independently and in parallel. An
event is marked as published only after the stream has durably accepted it. If an event fails, later
events of the same partition wait until it succeeds or is moved to dead letter after the retry limit.

**Why this priority**: publishing in correct order is the core business value requested.

**Independent Test**: with one publisher instance, publish a generated batch and consume the stream;
verify every event arrives, order per partition matches sequence numbers, and the store shows every event
as published.

**Acceptance Scenarios**:

1. **Given** pending events across all partitions, **When** the publisher runs, **Then** every event is
   published and consumers see each partition's events in ascending sequence order.
2. **Given** an event whose publishing fails, **When** retries are below the limit, **Then** no later
   event of that partition is published before it, and other partitions keep publishing.
3. **Given** an event that has failed the maximum number of retries, **When** the limit is reached,
   **Then** the event is recorded as dead letter with its error details and the partition continues with
   the next event.
4. **Given** a batch whose publishing was rejected by the stream, **When** the publisher records results,
   **Then** none of that batch's events are marked as published and only the actually failed event has
   its retry count increased.
5. **Given** the target stream has fewer partitions than expected or cannot be inspected, **When** the
   publisher starts, **Then** it stops with a clear error and publishes nothing.
6. **Given** an order with several lifecycle events, **When** they are consumed, **Then** all of them are
   in the same partition and arrive in lifecycle order.

---

### User Story 3 - Run several publisher instances safely (Priority: P2)

The operator runs more than one publisher instance to increase throughput and resilience. Instances share
the partitions: at any moment each partition has at most one valid owner. If an instance crashes, its
partitions are taken over by another instance after the ownership period expires. A former owner that
comes back cannot publish or update state for a partition it no longer owns.

**Why this priority**: required for throughput and fault tolerance, but a single instance already
delivers value.

**Independent Test**: run two instances, kill one abruptly, wait for its ownership to expire, and verify
that the surviving instance takes over, nothing is lost and order per partition is kept.

**Acceptance Scenarios**:

1. **Given** two running instances, **When** both try to own the same partition, **Then** only one
   succeeds.
2. **Given** an instance crashed while owning partitions, **When** its ownership expires, **Then**
   another instance takes over those partitions and continues from the first unpublished event.
3. **Given** a takeover happened, **When** the former owner tries to publish, renew or update state,
   **Then** the attempt is rejected and it stops publishing for that partition.
4. **Given** an instance loses ownership in the middle of a batch, **When** it detects the loss,
   **Then** it stops sending new events for that partition immediately.

---

### User Story 4 - Add new fake entity types later (Priority: P3)

The project will later generate other kinds of data (for example customers or products). Adding a new
entity type must reuse the same storing, publishing, ordering, retry and ownership behaviour without
changing it; the new entity only declares its fields, its partition key and its lifecycle.

**Why this priority**: only orders are needed now; extensibility protects future work.

**Independent Test**: review that the definition of what an "order" is lives apart from the publishing
pipeline, and that a sample second entity type can be described without changes to publishing,
ordering or ownership rules.

**Acceptance Scenarios**:

1. **Given** a new entity type is defined, **When** it is generated, **Then** its events flow through
   the same pending-event, ordering and publishing rules as orders.

---

### Edge Cases

- The stream accepts a batch but the publisher crashes before marking it published: after takeover the
  batch is published again (duplicate); consumers must detect and ignore it.
- The stream rejects one event in a concurrently sent batch: only that event is treated as failed, not
  earlier successful ones.
- Ownership renewal fails during a batch: publishing for that partition stops at once.
- The stream has fewer partitions than configured: the publisher refuses to start.
- A dead-lettered event is never published again and never picked again for publishing.
- More publisher instances than partitions: extra instances stay idle without error.
- An order is cancelled before shipping: its path ends at Cancelled and no later lifecycle event is
  generated for it.
- The configured lifecycle is invalid (no start state, unreachable states, or transitions to unknown
  states): the generator refuses to start with a clear error.
- The event target is reached in the middle of an order's lifecycle: the run stops at exactly the
  target; the order's remaining steps are not generated and this is reported.
- Settings differ between instances (target stream, partition count, identity namespace): treated as a
  misconfiguration and documented as unsupported.

## Requirements *(mandatory)*

### Functional Requirements

**Generation**

- **FR-001**: System MUST generate realistic fake orders and their lifecycle events up to a configurable
  total number of events (target run: 500,000 events).
- **FR-002**: System MUST store each record change together with exactly one pending event in the same
  unit of work, so that a change is never stored without its event or the reverse.
- **FR-003**: System MUST assign each event a partition derived from its entity's configured partition key
  (for Order: the customer), so all events with the same key share one partition, and a sequence number
  that is unique and strictly increasing within each partition (derived from a single global increasing
  sequence; gaps within a partition are allowed).
- **FR-004**: The definition of a fake entity (its fields, how values are generated, its partition key and
  its lifecycle) MUST be separate from the storing and publishing pipeline, so new entity types can be
  added without changing publishing, ordering, retry or ownership behaviour.
- **FR-023**: The order lifecycle MUST be a configurable state machine. Default: Created → Paid → Shipped
  → Delivered, with Cancelled allowed only from Created or Paid. Each order follows one randomly chosen
  valid path. An invalid configuration MUST stop the generator before any data is written.
- **FR-024**: Events of one order MUST be generated in valid lifecycle order, and each event MUST carry
  its lifecycle step (event type) so consumers can tell the steps apart.

**Ordering and publishing**

- **FR-005**: System MUST publish events of each partition in ascending sequence order.
- **FR-006**: System MUST process partitions independently and concurrently; no global ordering across
  partitions is required.
- **FR-007**: A failed event MUST block later events of the same partition until it is published or
  dead-lettered.
- **FR-008**: System MUST mark events as published only after the stream has durably committed them, and
  MUST mark a whole successful batch in one grouped operation.
- **FR-009**: When a batch fails, system MUST discard it as a whole on the stream side, mark none of it
  as published, and increase the retry count only of the event that actually failed.
- **FR-010**: After the configured maximum retries, system MUST record the event as dead letter with
  enough error detail for later investigation, then continue with the next event of the partition.
- **FR-011**: Published and dead-lettered events MUST never be selected for publishing again.
- **FR-012**: Before publishing, system MUST verify that the target stream exists and has at least the
  expected number of partitions (6), and MUST stop with a clear error otherwise or when this cannot be
  checked.

**Multiple instances and ownership**

- **FR-013**: System MUST support a configurable number of publisher instances and a configurable upper
  limit of partitions per instance.
- **FR-014**: Partition ownership MUST be exclusive and time-limited; claiming MUST be atomic and judged
  against one shared authoritative clock.
- **FR-015**: Every change of owner MUST produce a new, higher ownership token; renewing, releasing and
  every ownership-sensitive state change MUST be accepted only with the current owner and token.
- **FR-016**: When renewal fails, the instance MUST stop publishing for that partition immediately.
- **FR-017**: A former owner MUST be prevented from publishing to the stream as well as from changing
  state, independently of the ownership record (stream-level fencing).
- **FR-018**: Partitions of a crashed instance MUST become claimable once ownership expires.

**Delivery guarantee and reporting**

- **FR-019**: System MUST provide at-least-once delivery and MUST NOT claim exactly-once delivery.
- **FR-020**: Every event MUST carry a unique event identifier so consumers can detect duplicates; the
  consumer duplicate-handling contract (read only committed events, record processed identifiers with a
  uniqueness rule in the same unit of work as the business effect, skip already-seen identifiers) MUST be
  documented.
- **FR-021**: Each run MUST report counts of generated, published, dead-lettered and duplicate events and
  the achieved throughput.
- **FR-022**: All operational settings (target stream, partition count, batch size, send delay, retry
  limit, ownership timings, instance identity namespace) MUST be configurable without code changes.

### Key Entities *(include if feature involves data)*

- **Order**: a fake customer order; has an identifier, customer reference, amounts, current lifecycle
  status and creation time. First fake entity type.
- **Fake Entity Definition**: describes one kind of fake data: fields and value rules, partition key and
  lifecycle state machine; Order is the first one.
- **Pending Event (Outbox Event)**: one event per record change (lifecycle step); has event identifier,
  event type (lifecycle step), entity reference, partition key, partition, sequence number, payload, retry
  count, last error, published time and dead-letter time.
- **Partition Ownership (Lease)**: per partition; has current owner, expiry time, ownership token and last
  update time.
- **Run Report**: result of a run; counts of generated, published, dead-lettered and duplicate events and
  throughput.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A run of 500,000 generated events ends with 100% of them either published or dead-lettered;
  0 events lost.
- **SC-002**: 0 ordering violations within any partition, verified by consuming the full stream.
- **SC-003**: After an instance is killed abruptly, its partitions resume publishing within one ownership
  expiry period plus one renewal interval, with no lost events.
- **SC-004**: 100% of duplicates caused by a crash are detectable by event identifier, and a consumer that
  follows the documented contract applies each event's business effect exactly once.
- **SC-005**: In 100% of attempts, a stale owner fails to publish, renew or change state after takeover.
- **SC-006**: Starting against a stream with fewer than 6 partitions stops the publisher in 100% of cases
  before any event is sent.
- **SC-007**: Every run produces a report with generated, published, dead-lettered and duplicate counts
  and throughput, all backed by real execution.
- **SC-008**: A second sample entity type can be described and generated without any change to the
  publishing, ordering, retry or ownership behaviour.
- **SC-009**: Throughput has no fixed minimum: the first full 500,000-event run in a given environment
  sets the baseline, and a later run in the same environment that is more than 20% slower fails.
- **SC-010**: For 100% of orders without a dead-lettered event, the consumed events form a valid path of
  the configured lifecycle; orders with a dead-lettered event are reported separately as dead-letter gaps.

## Assumptions

- The event streaming platform is Kafka with 6 partitions; the data store is the existing relational
  database. Both are named only as dependencies, not as design choices of this spec.
- Only orders are implemented in this feature; other entity types are future work (FR-004 only requires
  the extension point).
- Only one generator instance runs at a time; concurrent generators need a separately designed sequence
  allocator and are out of scope.
- The requirements of `docs/spec.md` (multi-instance publisher engineering issue) are included in this
  feature and expressed here in business terms.
- Dead-lettered events are not published to the main stream; changing this needs a separate approved
  decision.
- The message contract changes to carry order lifecycle events (approved in Clarifications); any other
  contract change needs separate approval.
- The runtime environment for throughput measurement is not fixed; baselines are per environment.
- Out of scope: exactly-once between data store and stream; automatic change of the stream's partition
  count; message contract changes other than the approved lifecycle events; deleting dead-letter records;
  changing existing consumers without a compatibility review.
- Primary users are developers, testers and load-test engineers; no end-user UI is required.

## Approved v2 amendment — 2026-11-22 (authoritative for new work)

Earlier sections describe v1 ingress behavior. Where they conflict, this amendment
and contracts/final-topic-v2.md govern the approved final-topic pipeline.
Implementation is pending. Design: ../../docs/exactly-once-final-topic-design.md.

### US5 — Certify exact event equality (P1)
As an operator I require all generated events, including those not marked published,
to match the final topic exactly. Independent test: reject a same-count stream with
one missing event and one extra event, and reject duplicates/malformed records.

### US6 — Publish unique final events (P1)
As a consumer I receive one committed final record per generated EventId, even if
SQL-to-ingress replays. Independent test: replay an ingress batch after commit and
restart the processor; final identities, payloads and order remain exact.

### US7 — Recover and migrate safely (P1)
As an operator I retain correctness through process failure, rebalance and state
restore. Independent test: deterministic failures at transaction boundaries followed
by full reconciliation; incompatible topology/state reset refuses startup.

- FR-025: Closed runs MUST satisfy GeneratedEvents=FinalRecordsRead=FinalUniqueEvents.
- FR-026: Missing, unexpected, duplicate, malformed, payload mismatch, pending and
  dead-letter counts MUST all be zero for strict pass; verification must be complete.
- FR-027: Recording an event as delivered and making its final output visible MUST
  be one indivisible outcome. Recovery MUST neither lose an event nor expose it twice.
  Replayed identities MUST retain their original content and routing.
- FR-028: RunId MUST persist across generator restart; new runs get new identities.
  Immutable EventId and RunId headers MUST survive ingress and final publication.
- FR-029: Six fixed ingress/final partitions and original key/partition/order MUST
  be preserved. Duplicate identity with conflicting payload MUST stop the partition.
- FR-030: Dedup state and data retention MUST cover replay/verification horizons;
  incompatible reset, routing changes and incomplete verification cannot pass.
- FR-031: The stored Published flag means ingress acknowledgement only. Reports MUST separately
  measure final records and compare all generated IDs and payloads bidirectionally.
- SC-011: A closed 500000-event run has exactly 500000 committed final records and
  identities, with all FR-026 errors zero, including deterministic crash scenarios.
- SC-012: Restore/rebalance/fencing tests preserve equality, order and lifecycle.
- SC-013: For a single abrupt termination of either an ingress publisher or a final
  processor, with another eligible instance already running and backlog on every
  affected partition, final delivery MUST resume on every affected partition within
  18 seconds. Measure from acknowledged process termination to the first newly visible
  committed event on each affected partition; the maximum interval determines pass.
  Record observation uncertainty; an upper bound above 18 seconds cannot certify pass.
  Dependency outages are measured separately and must preserve correctness after recovery.

Dead-letter continuation remains ingress behavior; strict final acceptance fails
until every generated event is repaired and delivered in valid order. Repair that
would violate final ordering requires rebuilding a fresh versioned final pipeline.
Equality applies after draining, before retention expiry, with read_committed;
physical offsets and ingress record counts are not final business-message counts.

### Specification refinements — 2026-11-22

These requirements resolve behavioral questions from analysis U1–U6 and I2.
Technical mechanisms and task ordering remain planning work; no implementation
success is implied. Existing FR/SC identifiers and v1 history are preserved.

#### User scenarios and acceptance

**US5 acceptance**: Given two completed runs sharing a destination, verification
of run B counts only B's events and reports run A's recognized events separately.
Unknown run identities and malformed records in the audited destination fail the
audit. Given equal raw counts but one missing and one extra B event, verification
fails. Changing content under an existing identity also fails.

**US6 acceptance**: Given identical repeated events, including after restart, only
one final event is visible. Given the same identity with changed content, key or
partition, publication is rejected and the run fails; it is never silently accepted
as a different event. The check applies even when the attempted partition changes.

**US7 acceptance**: Given a crash after the last generated event but before run
closure, restart closes the existing run without adding events. Given a closed run,
restart alone does not start another run; an explicit new-run request creates a new
identity and generates its requested count independently of older stored events.
Given missing delivery history or changed pipeline identity with existing output,
startup fails before new output is emitted. A deliberate rebuild uses a fresh output
history and is approved separately.

#### Additional functional requirements

- **FR-032**: Verification MUST select an explicit RunId and an immutable closed-run
  manifest of every generated event, including unpublished and dead-lettered events.
  Capture a fixed end boundary for every destination partition and audit all retained
  business records from the run's registered audit start through those boundaries.
  Recognized other-run events are reported separately and excluded from selected-run
  counts; unknown run identities, invalid identities and malformed records fail audit.
  Boundaries MUST cover the run's entire output history; expired required history or
  an unclosed run makes verification incomplete, never successful.
- **FR-033**: Each run MUST persist its identity, target, generated count and status.
  At most one run may actively generate. Entity changes, event creation and progress
  MUST agree after any restart. Closure requires exactly the target count and freezes
  identity/content/routing, while delivery-status updates remain allowed. Target
  counts apply per run, and existing legacy data requires an explicit legacy identity.
- **FR-034**: The system MUST retain a durable binding between pipeline identity,
  destination history, routing version and duplicate-detection history. Missing or
  mismatched history MUST prevent publication. A new identity MUST NOT bypass this
  check against an existing destination.
- **FR-035**: Content comparison MUST follow one versioned equivalence contract across
  all components. Object-member order is irrelevant; array order and string contents
  are significant. Equivalent numeric values compare equal without loss of decimal
  precision. Duplicate object-member names and unsupported numeric values are invalid.
  Changing the comparison version requires compatibility validation before publication.
- **FR-036**: Each EventId MUST retain one RunId, content, key, sequence and partition
  across all retries. Routing validation MUST reject inconsistent copies before final
  output, including copies sent to a different partition. Per-partition deduplication
  alone is insufficient evidence of compliance.
- **FR-037**: Performance baselines MUST identify delivery-contract version, pipeline
  configuration and environment. A v1 ingress baseline MUST NOT be compared with v2
  final delivery. Only a fully successful strict run may establish a v2 baseline.

#### Key entities

- Generation Run: stable identity, requested target, progress and active/closed state.
- Closed Manifest: immutable identities, content and routing of all events in a run.
- Audit Scope: selected run, registered start boundaries, captured end boundaries,
  retained-history availability and recognized run identities for the destination.
- Pipeline Identity: durable association of output history, routing, comparison
  version and duplicate-detection history, retained across restarts.

#### Measurable outcomes

- **SC-014**: Two consecutive runs in one nonempty environment each produce exactly
  their own requested count; auditing either run reports zero missing/duplicate/extra
  events and excludes the other recognized run from that run's count.
- **SC-015**: All tested interruptions at event-write, progress and closure boundaries
  preserve the run identity and exact target; restart creates zero unintended runs.
- **SC-016**: Every tested missing-history, identity-reset or changed-routing attempt
  is rejected before it adds final output; all content-equivalence fixtures agree
  across components, including precision and Unicode cases.

#### Assumptions and scope

- Multiple historical runs may share a destination; only one generator is active.
  Explicit run selection and durable run registration are required for that use.
- A valid recognized event from another run is not an unexpected event of this run;
  unknown or malformed destination data prevents certifying the audit as trustworthy.
- Required history is retained through reconciliation. Permanent storage loss and
  deliberate expiration are detected limitations, not successful no-loss outcomes.
- The approved Kafka Streams architecture remains a constraint in the constitution
  and plan. Algorithms, versions, deployment commands and implementation dependencies
  belong to planning, not to these behavioral acceptance criteria.
