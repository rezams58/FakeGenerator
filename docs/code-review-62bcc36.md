# Code review against 62bcc36

Review scope: committed changes from `62bcc365b1d531af913fb371d45f734fa666ea3f` to HEAD, plus current working-tree and new source/test files. The baseline spans lifecycle generation, fenced publishing and run verification. Independent Standards and Spec reviews were performed using the `code-review` skill.

## Standards

Two correctness findings against `.specify/memory/constitution.md`:

1. **P1 — Spare publisher capacity did not claim newly expired partitions.** Waiting for every existing lane indefinitely prevented another ownership claim. The scheduler now maintains active lanes and repeatedly claims only the remaining capacity. A regression test initially timed out with the original scheduler.
2. **P1 — Lowering MaxRetryCount could bypass an unresolved event.** The old query excluded an exhausted event without giving it a terminal state. A fenced SQL update now finalizes exhausted rows before successor selection, preserving the retry count and original error. The regression test initially showed that the skipped event was not dead-lettered; it also checks rejection of a stale fencing token.

No cosmetic heuristic findings are treated as blockers.

## Spec

Five findings against SC-001, SC-002, SC-009, SC-010 and the report measurement contract:

1. **P1 — Incomplete runs could pass.** Verification success must include the configured generation target and resolution of every event, rather than only checking the already-published subset.
2. **P1 — Partial stream reads could pass.** Inactivity before reaching the captured partition ends must be reported as incomplete consumption and fail acceptance. Cancellation must not return a successful partial result.
3. **P2 — Small trial runs could create the 500K baseline.** Only qualifying 500,000-event samples should establish or be compared with that performance baseline.
4. **P2 — Dead-letter gaps could be omitted.** A terminal dead-letter or an entirely dead-lettered order must be counted from the SQL affected-order set, even when its consumed prefix contains no broken transition.
5. **P2 — GeneratedThisRun could be fabricated from an unrelated previous report.** During implementation, report inspection exposed that subtracting the last report's total could silently describe a different database or a reset. This additional measurement defect was fixed explicitly: the field is nullable when no generation-start count was captured. `BuildAsync`/`Complete` accept an optional start count for callers that measured it. Existing CLI calls remain source-compatible, but JSON consumers must allow null for `GeneratedThisRun`.

All five were corrected. `RunReportWriter.Complete` checks exact target and reconciled final counts, requires verification, and restricts baseline comparison/creation to complete 500K samples. `VerificationResult.Complete` and `RunReport.VerifierComplete` expose consumption completeness; inactivity fails closed, cancellation throws, completed partitions are paused, and records beyond captured end offsets are excluded. Dead-letter gap counts are based on the full SQL affected-order set. Expected at-least-once replays remain non-failing.

## Verification status

- Both publisher regression tests failed before the corresponding production fixes: one timed out waiting for spare-capacity takeover, the other found an exhausted event without DeadLetterAtUtc (`reports/code-review/publisher-red.log`). Both passed afterwards.
- Reporting scoped red/green cycle: 18 failing / 14 passing tests before fixes, then 32 passing reporting/baseline tests after fixes.
- Final full suite: **130 passed, zero failed, zero skipped**, including real SQL/Kafka integration tests (`reports/code-review/final-tests.log`).
- Final Release solution build: **zero warnings, zero errors** (`reports/code-review/final-build.log`).
- `git diff --check` passed.
- Retained-data Kafka verification passed: the revised `--verify` CLI consumed a six-partition replay of **500,000 published events**, reported `VerifierComplete=true`, zero ordering/lifecycle/missing/duplicate findings, and exited **0**. Evidence: `reports/code-review/replay-verification.log`, `reports/code-review/replay-20261118105823/replay-evidence.json`, and `run-20261118T110003017Z.json` in that directory.

## Existing runtime evidence

The preceding 500K run, documented in [e2e-validation.md](e2e-validation.md), passed with zero ordering/lifecycle/missing findings and 108 tests. It predates this review's changes and must not be represented as a fresh run of the revised publisher.

Attempting to verify that original topic failed with `Unknown partition`; broker metadata no longer contained the original six-partition topic. This failure is retained in `reports/code-review/retained-stream-verification.log`. The database still contains the 500K published events. A separate validation replays those published rows, unchanged EventIds and per-partition sequence order, into a new six-partition topic and runs the actual revised `--verify` CLI. The SQL source is read-only during replay. This is verification of retained data, not a new generation/crash benchmark or a second independent performance sample.

The replay topic is `fakegen-review-verify-20261118105823` and is retained for inspection. The report's publish-window/throughput fields still describe the original SQL publication timestamps; they are not measurements of replay performance. `GeneratedThisRun=null` correctly indicates that this separate verifier process did not capture a generation start count.

## Completion

**T063 completed.** Standards: 2 confirmed hard correctness findings, both fixed; the highest original severity was P1. Spec/reporting: 5 confirmed correctness findings, all fixed; the highest original severity was P1. No confirmed finding from this review remains open. All 63 implementation tasks are complete; the deployment/performance limitations below remain explicitly outside the evidence.

## Residual scope

- Single-generator support and short shared-table-lock trade-offs remain unchanged.
- Production deployment and a new post-review 500K generation/crash benchmark are not run. Current scheduler behavior is covered by the new bounded-capacity regression plus the real lease/Kafka E2E suite.
- The review itself did not commit, merge or deploy changes; subsequent Git/PR preparation is a separate step.

## Files and resulting architecture

- `src/Workers/KafkaPublisherWorker.cs`: bounded, continuously replenished partition-lane scheduler; shutdown cancels and awaits active lanes.
- `src/Data/SqlOrderRepository.cs`: lease-fenced retry exhaustion finalization before ordered pending selection.
- `src/Reporting/RunReportWriter.cs`, `StreamVerifier.cs`, `StreamVerifierCore.cs`, `RunReport.cs`: explicit completion/acceptance policies, captured-offset stream verification, valid baseline eligibility and truthful optional generation counts.
- Regression coverage: `tests/FakeGenerator.Tests/Unit/PublisherWorkerTests.cs`, `Fakes.cs`, `StreamVerifierLogicTests.cs`, new `RunReportWriterTests.cs`, and `tests/FakeGenerator.Tests/Sql/OutboxSelectionTests.cs`.
- Documentation/status: `src/README.md`, `IMPLEMENTATION_NOTES.md`, `docs/e2e-validation.md`, this report and feature `tasks.md`.

The entity definition/writer/generator pipeline and transactional outbox architecture remain in place. Each publisher lane retains its SQL lease/fencing token and stable per-partition Kafka transactional producer; Kafka commit still precedes the bulk SQL published mark. Review changes repair capacity scheduling, terminal-state handling and verification without claiming distributed exactly-once delivery.
