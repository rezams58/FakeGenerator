# Final-topic v2 contract

Approved 2026-11-22; supersedes v1 final-stream duplicate acceptance.

Phase 1 refinement: [v2-processing.md](v2-processing.md) is authoritative for run
scope, canonical encoding, catalog validation, reset guards and CLI/configuration.
In particular its decimal-json-v1 grammar supersedes the draft compact-JSON rule below.

- Ingress orders.ingress.v2 and final orders.final.v2 each have exactly six fixed
  partitions. Source events preserve customer/aggregate key and explicit partition.
- Required headers: run-id (UUID), event-id (UUID), sequence-no (positive integer),
  event-type (nonempty). Run identity is persisted in SQL and immutable per event.
- SQL run state: Active -> Closed. Close only at the event target after atomic
  entity/Outbox writes complete. Persist target and creation/closure timestamps.
  Restart loads the active run; verifier requires Closed and an immutable manifest.
- Dedup key: (RunId, EventId), local to the stable source partition. Persist canonical
  payload fingerprint plus partition/key/type/sequence; conflicting replay fails.
  Canonicalization recursively sorts JSON object properties ordinally, retains array
  order, emits compact UTF-8 and hashes SHA-256. Define cross-language fixtures for
  numeric/string encoding before implementation; no platform-specific hash defaults.
- Logged state store, output and input offsets commit together under Streams EOS v2.
  No state eviction within replay horizon; no reset with an existing final topic.
- Finalizer forwards immutable payload, headers, key and source partition. Only it
  is authorized to write final topic. Consumers and verifier use read_committed.
- Final topic is event history (no compaction), retained through verification horizon.
  Ingress/Outbox retained until reconciliation; state survives local disk loss through
  changelog restore. Internal replication and broker compatibility are preflight gates.
- Malformed/unknown-run/wrong-partition/conflicting events fail the run; known other-run
  records are validated and counted separately under v2-processing.md. Do not silently
  skip or certify a dead-lettered event. End-to-end external SQL effects still need
  transactional Inbox handling even when final Kafka records are unique.
- Strict verifier reads every committed record to captured ends and compares against
  ALL generated events, not only SQL-published IDs. Raw business-record count includes
  malformed records. It checks both set differences and payload/key/partition/sequence.
- Required report fields: GeneratedEvents, IngressAcknowledged, FinalRecordsRead,
  FinalUniqueEvents, Duplicates, MissingGeneratedEvents, UnexpectedEvents,
  InvalidRecords, PayloadMismatches, Pending, DeadLettered, VerificationComplete.
  Only exact equality plus zero errors/order/lifecycle violations passes. No baseline
  for incomplete/failed runs. Historical v1 reports retain their historical meaning.
- Rollout uses fresh versioned topics; old duplicate history cannot be repaired in
  place. Consumer cutover is a separately approved operation after reconciliation.
