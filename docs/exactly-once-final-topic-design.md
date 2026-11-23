# Exactly-once final-topic proposal

Status: design for approval; implementation has not started. 2026-11-22.

## Requirement and evidence

For a closed generation run, every committed Outbox event must occur exactly once
in the final business topic, with identical identity and payload, per-partition
ordering, no missing events and no foreign events. Count events, not Orders rows:
one order has several lifecycle events.

Independent audit of run 20261122050718: SQL 500000; committed Kafka records 500500;
unique EventIds 500000; duplicates 500; missing/foreign IDs zero. The existing
verifier accepts duplicates and compares missing IDs only against SQL-published
rows. It also skips some malformed records. Its Passed flag cannot certify this
new requirement.

Kafka commit and SQL MarkPublished are separate operations. A crash in between
leaves committed records eligible for replay. Producer idempotence does not
deduplicate application replays in new transactions by EventId. Marking SQL first
would instead risk loss. SQL Published currently means the outbox sender marked
delivery, not that a strict final-topic contract was verified.

## Recommended architecture

SQL Outbox -> existing .NET sender -> orders.ingress.v2
-> durable transactional deduplicator -> orders.final.v2 -> consumers.

Use a small Kafka Streams service with processing.guarantee=exactly_once_v2,
a logged persistent state store and a stable application.id. This adds a JVM
service to the .NET deployment. Verify actual broker/client compatibility before
selecting and pinning dependency versions.

Do not repartition by EventId: preserve the six source partitions and existing
aggregate/customer key. All copies of an event must retain their original
partition; validate run identity, partition mapping and immutable EventId/payload.
Each partition's state store maps (RunId, EventId) to a canonical payload hash.
For an unseen event, store its identity/hash and forward the unchanged event to
the same final partition. For a seen identical event, produce no business record.
For a seen identity with different contents, fail the run and stop that partition;
never silently discard a conflicting payload.

State changelog, final output and consumed ingress offsets must commit in the
same Kafka transaction through Streams. On restart/rebalance, restore committed
state before processing; aborted changes must not survive in local state. EOS
alone does not suppress two distinct ingress offsets with the same EventId: the
explicit persistent deduplication store is mandatory.

Per-partition state assumes stable routing: one event cannot migrate partitions.
Partition-count or routing changes require a new versioned pipeline and migration,
not an online configuration toggle. Validate the routing field in every record.
Only the deduplicator may write the final topic; restrict other producers via ACLs.

The guarantee applies to committed business records read with read_committed.
Ingress may contain duplicates. Internal topics carry state and offsets. Physical
broker records/offset differences are not business-message counts (transaction
markers and aborted records exist). This proposal does not promise duplicate-free
ingress or physical log storage.

## Durability and failure boundaries

Retain Outbox and ingress until final reconciliation succeeds. Retain dedup state
for the entire possible replay horizon; initially no time-based eviction. Use
compacted changelogs without expiry/tombstoning of live identity entries. Capacity
must be sized for retained events. Never reset application.id/state while keeping
the existing final topic and allowing historical input replay.

Set output retention to cover the complete verification/consumer horizon; disable
compaction of the event-history output. Retention expiry intentionally removes
records and invalidates an all-history count comparison. Run verification on a
closed run before expiry. Start with distinct topics per acceptance test.

Retries are bounded per attempt but exhausted retries cannot count as successful
delivery. A poison/dead-lettered event makes strict acceptance fail; repair and
replay it before certifying equality. When underlying durable storage is lost,
retention expires or administrators delete topics/state, this design cannot claim
unconditional no-loss delivery. After recoverable outages, convergence is checked
only once all stages drain; equality is not expected during active production.

## Verifier and reporting changes

Freeze a closed-run SQL manifest: RunId, EventId, SequenceNo, partition, key and
canonical payload hash for ALL generated events. Capture Kafka partition ends and
read all committed business records through those ends, including malformed ones
in total counts. Compare sets in both directions and verify hashes, key/routing,
ordering and lifecycle. Unknown/malformed records fail instead of disappearing.

Report GeneratedEvents, IngressAcknowledged, FinalRecordsRead, FinalUniqueEvents,
Duplicates, MissingGeneratedEvents, UnexpectedEvents, InvalidRecords,
PayloadMismatches, Pending, DeadLettered and VerificationComplete separately.
Do not rename current SQL Published to FinalPublished without measuring the final
topic. Count equality alone is insufficient (one missing plus one extra can cancel).

Strict pass requires GeneratedEvents=FinalRecordsRead=FinalUniqueEvents=target,
exact identity/payload agreement, all error counters zero, pending/dead-letter zero,
ordering/lifecycle checks passing and verification reaching all captured ends.
Baseline creation must require strict pass. Historical Passed reports remain
historical at-least-once results and must not be retroactively relabeled strict.

## Acceptance matrix

1. Normal 500000-event, six-partition run: exact counts/sets/hashes and zero errors.
2. Deterministic crash immediately after ingress commit and before SQL mark:
   duplicate ingress exists; final topic still has each EventId once.
3. Duplicate injection at later offsets, within one batch and after restart:
   identical events suppressed; different payload under same identity fails.
4. Kill deduplicator before commit, after commit/before response, and after local
   state mutation: restore/replay yields no missing or duplicate final records.
5. Rebalance, concurrent instances, old-owner fencing and complete local-state
   deletion with intact changelog: restore and preserve final count/order.
6. SQL outage, broker outage and ambiguous commit acknowledgement: recover then
   reconcile all generated IDs without assuming an exception means an abort.
7. Poison event / exhausted retries: strict pass false until event repaired/replayed.
8. Malformed headers/payload, foreign event, wrong routing and same-count
   missing-plus-extra substitution: each fails verification.
9. Retention/state reset or incompatible partition configuration: startup/preflight
   fails or guarantee marked unavailable, never a successful strict report.
10. Verification timeout/cancellation, partial partition consumption, concurrent
    unclosed generation: no strict pass or baseline creation.

Inject failures using deterministic barriers, not random process timing. Record
SQL manifest, ingress/final partition+offset evidence and commit outcome in each
case. Run focused failures first, then full workload; measure added latency and
recovery against the existing 18-second target (not yet proven for this design).

## Alternatives and rollout

Consumer SQL Inbox provides idempotent effects but leaves duplicate Kafka records.
Compaction is asynchronous and is not exactly-once event-history delivery.
A .NET-only transactional receipt/checkpoint protocol could avoid the JVM service,
but needs a separate proof for fencing, recovery barriers, uncertain commits and
retention; it is not proposed as a quick patch to the current publisher.

After design approval: update feature requirements/contracts and constitution
where they explicitly mandate at-least-once consumer deduplication; enumerate
implementation tasks. Implement strict verifier first so old duplicate scenarios
fail, then ingress/final configuration and the processor, then fault tests.
Deploy to fresh versioned topics in an isolated environment, reconcile completely,
then obtain approval for consumer cutover. Keep old topic/evidence intact; existing
duplicates cannot be removed in place to establish the new contract. Any rollback
must preserve final-topic/state consistency and may pause delivery rather than
revert consumers to the duplicate ingress topic.

## Sources

- https://kafka.apache.org/design/ — Kafka transaction boundaries and read_committed.
- https://kafka.apache.org/39/streams/developer-guide/config-streams/ — EOS v2 config.
- https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=68721354 —
  KIP-129 atomic state, output and input-offset processing.

The pipeline above is a proposed application of these primitives, not an assertion
that the current project already implements exactly-once publication.
