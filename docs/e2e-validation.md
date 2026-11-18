# 500K E2E validation — 2026-11-18

Final result: **passed**, with a clean database and a dedicated six-partition Kafka topic.

## Final measured output

| Measurement | Actual |
|---|---:|
| Generated events | 500,000 |
| Published events | 500,000 |
| Dead-lettered / pending | 0 / 0 |
| Orders generated | 142,750 |
| Incomplete lifecycle prefixes at the event target | 8,629 |
| Duplicate deliveries in this run | 0 |
| Ordering / lifecycle violations | 0 / 0 |
| Dead-letter gaps / missing published events | 0 / 0 |
| Orders spanning multiple partitions | 0 |
| Publish window | 361.109 seconds |
| Throughput | 1,384.62 events/second |
| Crash takeover | 14.1176529 seconds |
| Configured lease / renewal / takeover bound | 15 / 3 / 18 seconds |
| `--verify` exit code | 0 |
| Tests | 108 passed, 0 failed, 0 skipped |
| Release solution build | 0 warnings, 0 errors |

The first valid full run created `baseline.local-500k.json`. A second valid 500K performance sample was **not run**, so no measured regression percentage is claimed. The comparison boundary is covered by the executed baseline unit tests.

## Execution and retained evidence

- Database: `FakeGenE2E_20261118092920`.
- Topic: `fakegen-e2e-20261118092920`, six partitions.
- A ran the only generator plus a publisher capped at three partitions; B ran a publisher capped at three. Both owned three partitions before B was forcibly killed. C replaced B, using the same producer namespace, and acquired B's partitions after expiration.
- The generator used target 500,000, batch size 5,000, seed 42, and interval 250 ms. Publisher lease duration was 15 seconds and renewal interval was 3 seconds. These override quickstart defaults and are the parameters of the measured bound above.
- The three schema migrations succeeded twice on the clean database.
- The publishers were stopped after SQL reached 500,000 events and zero pending, then the actual application was run with `--verify` against this database and topic.
- Raw reports are under `reports/e2e-20261118092920/`: `evidence.json`, `additional-checks.json`, `run-20261118T093551898Z.json`, `baseline.local-500k.json`, and instance/verification/negative-check logs.
- Build output: `reports/final-build.log`. Test output: `reports/final-regression-tests.log`.
- Local runner sources: `reports/e2e-runner/` and `reports/e2e-checks/`. They read credentials from the existing environment or git-ignored `local.env`; credentials are not copied into this document.
- Retry/dead-letter checks used a separate database, `FakeGenE2E_20261118092920_Retry`, leaving the 500K dataset intact. Dedicated E2E databases/topics are retained for inspection. Individual integration tests delete their own temporary Kafka topics.

## Acceptance criteria from docs/spec.md section 12

| Criterion | Executed evidence / result |
|---|---|
| .NET 8 restore | Runner restore and application/test builds succeeded against net8.0. |
| Successful build | Release solution build: zero warnings and errors. |
| Unit tests pass | Full suite: 108 passed, no skipped or failing tests. |
| Migration on a clean database | Three migrations applied twice successfully before generation. |
| SQL integration tests pass | Full suite includes atomicity, restart, lease concurrency/lifecycle, outbox selection and the new concurrent-read regression. |
| Kafka integration tests pass | Actual transaction publishing, topology rejection, fencing and both E2E tests passed. |
| Fewer than six partitions stops publisher | Additional check used a three-partition topic; application exited unsuccessfully with the expected topology error and SQL published count stayed 500,000. |
| Exclusive valid partition ownership | Concurrent claim tests passed; A and B each held three distinct partitions before the crash. |
| Recovery after crash | Forced B termination; C takeover measured at 14.1176529 seconds, below the configured 18-second bound. |
| Old producer fenced | Real Kafka fencing test passed with a shared transactional ID. |
| Per-partition ordering | Final read-committed stream verification: zero violations. |
| Failure blocks later events | Real SQL retry check selected the failed head before each retry; only after its terminal dead-letter state did the next event become selectable. Worker prefix/failure unit tests also passed. |
| Dead letter at MaxRetryCount | Three real `MarkFailedAsync` calls with max=3 produced RetryCount=3 and DeadLetterAtUtc; next event became selectable. LastError length was 2,000. |
| SQL mark follows Kafka commit | Executed worker tests verify call order and no mark on commit failure; real crash-before-mark deduplication E2E passed. |
| Consumer deduplication tested | `ConsumerDeduplicationE2ETests` passed against real SQL/Kafka using a new temporary topic. This is separate from the final 500K run, which happened to produce zero duplicates. |
| 500K throughput report | 1,384.62 events/second over a 361.109-second SQL publish window. |
| Counts reported | Generated 500,000; published 500,000; dead-letter 0; duplicate 0. |
| No unexecuted success claims | Raw outputs are retained; production deployment and a second full-run regression comparison were not executed. |

Additional negative lifecycle check: `InitialState=INVALID` caused options validation to fail before writes; Outbox count remained exactly 500,000.

## Defects discovered and fixed during validation

1. An initial 500K run exposed a real ordering failure: partition 1 delivered sequence 6,875 after sequence 8,019. Ordinary READ COMMITTED scanning can miss a concurrent insertion before the scan cursor. `ReadPendingAsync` now takes a short shared table lock with HOLDLOCK and uses the ordered pending index. Locks end before Kafka publishing. A range-lock-only attempt caused deadlocks and was rejected; that failed attempt is not the successful result above.
2. The initial verifier incorrectly failed expected at-least-once duplicate deliveries. Duplicates remain counted and excluded from ordering checks; they no longer fail a valid run. A regression assertion first failed on the old behavior, then passed after the fix. Invalid verification runs no longer create throughput baselines.
3. Shared historical Kafka topics made deduplication tests grow slower across repeated runs. Publisher/fencing/E2E tests now create isolated temporary six-partition topics and delete them afterwards.
4. Added `OutboxConcurrentReadTests`: concurrent inserts and ordered polling of 10,000 events verify no lower sequence is skipped. The final full suite passed with this regression test included.

The shared table lock trades write concurrency for a stable read. The measured throughput above includes that choice; production sizing and performance under other loads remain deployment-specific. The single-generator constraint remains in force.

## Remaining workflow work

T060 and T061 are complete. T063 is also complete; see [code-review-62bcc36.md](code-review-62bcc36.md) for the subsequent review, fixes and 130-test verification. The 500K generation/crash measurements above predate those review fixes. Post-review verification replayed the retained SQL events into a new topic because the original topic was no longer available; it was not another generation/performance run.
