## Summary

Fix ordering, recovery and verification edge cases found while validating the lifecycle publisher already on `main`.

```text
Outbox inserts -> stable ordered batch -> Kafka transaction -> fenced SQL mark
Publisher      -> continuously claim spare capacity after another owner's expiry
Verifier       -> exact target + resolved SQL counts + every captured partition end
```

- Prevent concurrent inserts from appearing halfway through ordered Outbox reads. Keep the short shared-table-lock trade-off explicit.
- Reclaim newly expired partitions while healthy lanes continue, without exceeding the per-instance cap.
- Finalize exhausted rows under the current fencing token when the retry limit is reduced, preserving their retry count and original failure.
- Fail incomplete generation, unresolved SQL totals and partial stream verification; restrict performance baselines to complete 500K samples.
- Count all SQL dead-letter-affected orders and report unmeasured `GeneratedThisRun` as null instead of inferring it from an unrelated report.
- Isolate Kafka integration topics and serialize tests sharing SQL state. Exclude local credentials, logs, test output and dumps from commits.

## Evidence

- **Before:** the initial 500K run reported one real ordering violation. Capacity/retry regression tests failed, and scoped reporting tests exposed incomplete-run and baseline errors.
  **After:** 130 tests passed with real SQL/Kafka, zero failures or skipped tests; Release solution build completed with zero warnings/errors. See [code review](code-review-62bcc36.md).
- **Before review fixes:** a clean 500K generation/crash run completed with 500,000 published, zero pending/dead-lettered/missing, zero ordering/lifecycle violations, and takeover in 14.118 seconds against an 18-second bound. Recorded throughput was 1,384.62 events/second. See [acceptance evidence](e2e-validation.md).
  **After review fixes:** the revised `--verify` CLI passed against a fresh six-partition replay of those 500,000 retained SQL events, with `VerifierComplete=true`, zero violations/missing and exit code 0. The original Kafka topic was unavailable. Replay was read-only against the SQL source and is not a new generation/crash or throughput benchmark.
- `git diff --check` passed. Raw execution artifacts remain local under ignored `reports/`.

## Merge Danger

**Door:** two-way

No destructive schema migration or distributed exactly-once claim is introduced. Consumers of report JSON must accept `GeneratedThisRun=null` when the start count was not captured; `VerifierComplete` is an additive field. Lowering the retry limit intentionally finalizes already-exhausted rows, so reverting code will not undo those terminal timestamps.

**Blast Radius:** publishing

Ordered reads briefly acquire a shared Outbox table lock; benchmark contention on the deployment SQL Server. Only one generator remains supported. Production deployment and a fresh post-review 500K generation/crash benchmark were not run.
