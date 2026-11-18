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
