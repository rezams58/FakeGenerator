# Implementation notes — multi-instance publisher

## Changed in this revision

- Replaced the shared, unclaimed Outbox polling loop with SQL-backed partition lease claims.
- Added `PublisherPartitionLease` ownership, expiry, heartbeat, and monotonically increasing fencing tokens.
- Fenced stale Kafka producers with one stable Kafka `transactional.id` per topic/partition. All deployed instances must share the same `Kafka:ClientId` namespace.
- Added lease checks to Outbox reads and to success/failure SQL updates.
- Changed publishing to one producer lane per owned partition. Records are enqueued in `SequenceNo` order, awaited as a batch, and committed as a Kafka transaction.
- On a delivery failure inside a batch, the aborted batch's successful prefix is committed separately; only the actual failed event receives a retry increment.
- Added terminal dead-letter state (`DeadLetterAtUtc`) after `MaxRetryCount`, without deleting the event.
- Added startup checks for topic metadata/partition count and the SQL lease rows.
- Removed per-message topic administration and the previous code that swallowed topic creation errors.
- Set producer acknowledgements to `All`, idempotence on, LZ4 compression, and configurable batching/linger.
- Removed temporary ServiceProvider construction from `Program.cs`.
- Added SQL lease migration, corrected monitoring queries for the current schema, and added unit/opt-in SQL concurrency tests.

## Important semantics

- Per-partition ordering is maintained for non-dead-lettered events. A terminal dead-letter event is skipped so later events can proceed; therefore it is not present in the main topic stream.
- Kafka transactions fence stale producers and make each Kafka batch atomic within Kafka. They do not create a distributed transaction with SQL Server.
- If Kafka commits but SQL does not update `PublishedAtUtc`, the event can be replayed. Delivery remains at-least-once; consumers must atomically deduplicate on `EventId` with their business write.
- SQL fencing protects database operations. Kafka transactional producer fencing is also used because a SQL token alone cannot prevent a stale process from writing to Kafka.
- The current order generator still uses `MAX(SequenceNo)` and is not safe to run in multiple generator instances. This revision targets multi-instance publisher ownership, not multi-instance order generation.

## Validation status (2026-11-18, real output)

- Latest Release solution build: **0 warnings, 0 errors** (`reports/code-review/final-build.log`).
- Latest full test suite with real SQL/Kafka and review regressions: **130 passed, 0 failed, 0 skipped** (`reports/code-review/final-tests.log`).
- The clean 500K run passed: **500,000 generated and published**, zero pending/dead-lettered/missing/ordering/lifecycle violations; `--verify` exited 0.
- Throughput: **1,384.62 events/second** over 361.109 seconds. First valid full-run baseline created.
- Two publishers initially owned three partitions each. After forcibly killing B, C took over in **14.1176529 seconds**, below the configured 15-second lease + 3-second renewal bound.
- Negative lifecycle and three-partition topology checks failed with their expected validation errors and no changes to the checked SQL counts.
- Real SQL retry/dead-letter checks passed at MaxRetryCount=3, including head-of-line blocking, advancing after dead letter, and LastError truncation to 2,000 characters.
- Full acceptance matrix, resource names, evidence locations, and defects fixed during the run: [docs/e2e-validation.md](docs/e2e-validation.md).
- T063 fixed-point review against `62bcc36` completed: two Standards findings and five Spec/reporting findings fixed. See [docs/code-review-62bcc36.md](docs/code-review-62bcc36.md).
- The revised verifier passed a 500K replay from the retained SQL data into a fresh six-partition topic: complete consumption, zero ordering/lifecycle/missing findings, exit 0. The original Kafka topic was unavailable; replay did not modify the SQL source or constitute a new generation/performance run.
- Not run: a fresh post-review 500K generation/crash benchmark, a second independent performance sample, and production deployment.

Outbox reads now take a short shared table lock to prevent concurrent inserts being missed during the ordered scan. The initial range-lock-only fix caused deadlocks and was rejected. The final shared-table-lock implementation passed the full 500K verification and 108 tests. Expected at-least-once duplicates are counted without failing verification; consumer Inbox deduplication remains required. Kafka integration tests use isolated temporary topics.

Review fixes add continuous capacity claims and fenced finalization of exhausted rows after retry-limit reductions. Reports now reject unresolved/short runs and partial stream reads, restrict baselines to 500K samples, count every SQL dead-letter-affected order, and return null for unmeasured `GeneratedThisRun` rather than inferring it from a potentially unrelated report. The 130-test result above covers these changes; the earlier 108-test/500K evidence is historical.
