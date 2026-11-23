# Contract: Consumer (deduplication, ordering, lifecycle)

The publisher gives **at-least-once** delivery. Every consumer of the topic MUST follow this contract.
`StreamVerifier` (`--verify`) follows the read-side rules too.

## Required consumer settings

- `IsolationLevel = ReadCommitted`, so records from aborted transactions are never seen.
- `EnableAutoCommit = false`. Commit offsets only after the business transaction has committed.

## Deduplication (Inbox)

```sql
CREATE TABLE dbo.Inbox
(
    EventId       UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_Inbox PRIMARY KEY,  -- unique constraint
    EventType     NVARCHAR(100)    NOT NULL,
    SequenceNo    BIGINT           NOT NULL,
    ReceivedAtUtc DATETIME2(3)     NOT NULL DEFAULT SYSUTCDATETIME()
);
```

Steps for each record:
1. Read `EventId` from the `event-id` **header**.
2. In one SQL transaction:
   - Insert the record into `dbo.Inbox`.
   - Run the business operation.
   - Commit.
3. A primary key violation on the Inbox insert (error 2627/2601) means the record is a duplicate.
   Skip it and commit its offset.
4. Commit the Kafka offset after the SQL commit.

## Ordering check

- Per partition, `sequence-no` MUST be strictly increasing. A duplicate re-delivery may repeat an
  earlier value, and the Inbox absorbs it.
- A value lower than the last processed one that is **not** a known duplicate is an ordering
  violation.

## Lifecycle check (SC-010)

- Group the events by `OrderId`, in `sequence-no` order. The sequence MUST be a valid path of the
  configured lifecycle (data-model.md §4):
  - It starts with `OrderCreated`.
  - Every step is an allowed transition.
  - After a terminal state nothing follows.
- A gap caused by a dead-lettered event is reported separately ("dead-letter gap"). It is not counted
  as a violation.

## Verifier output → run report

`duplicates`, `orderingViolations`, `lifecycleViolations`, `deadLetterGaps`, `missingOnStream`
(SQL published minus the distinct `event-id`s consumed).

## v2 applicability
The rules above describe at-least-once ingress and external SQL business effects.
For the approved final topic, final-topic-v2.md governs: duplicates fail acceptance.
Inbox still protects external effects against consumer reprocessing, but it is not
used to justify duplicate final Kafka records.
