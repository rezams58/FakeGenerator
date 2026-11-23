# T101 feasibility status — 2026-11-23

Status: INCOMPLETE. Negative control and steady-state two-thread candidate executed.
Separate threads allowed healthy progress in this bounded experiment. Shared-thread
independence failed as expected. U9 remains open and T101 unchecked.

Prepared `tests/feasibility/finalizer-isolation/pom.xml` with Streams 4.1.2,
Java 17, JUnit 5.11.4, compiler 3.13.0 and Surefire 3.5.2.
`IsolationTest.java` is a negative-control probe: two source partitions, one thread,
EOS-v2, a bounded blocking callback modeling local retry, committed output and group
offset observations, then release and verify both events arrive. A successful test
would demonstrate that this shared-thread model does NOT meet independence. It does
not establish a solution or simulate an actual SQL exception.

Initial execution attempt (historical):
- java/mvn absent from PATH; inspected common install roots yielded no usable runtime.
- Docker server 29.9.3 available with elevated access.
- `docker run --rm ... maven:3.9.9-eclipse-temurin-17 mvn -B test` exited 125:
  Docker Hub authentication endpoint connection refused while pulling the image.
- Local image inventory contains no Maven test environment.
- Log: `reports/isolation-probe.log` (local, ignored).
- Build, negative control, healthy-partition progress, heartbeat behavior, rebalance,
  takeover and candidate supported isolation-policy tests: **NOT RUN**.
- No container/test JVM started; this attempt created no Kafka test topics.

Prerequisite next step: make Java17/Maven available locally or pre-load the specified
Docker image with working dependency access. No system proxy configuration was changed.
Then run the negative control and investigate supported task assignment/transaction
isolation. Add actual exception, heartbeat/rebalance/takeover evidence before deciding
on a policy. Capacity/topology changes require separate architecture approval.

Run with process-local FAKEGEN_TEST_KAFKA; do not put credentials in tracked files.
The probe uses UUID-named input/output topics and removes them in its cleanup path.
Review teardown behavior under partial setup failure before repeated fault testing.

## Successful retry — 2026-11-23

User supplied maven:3.9.9-eclipse-temurin-17; image ID
sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc.
Docker run with the workspace probe mounted and process-local FAKEGEN_TEST_KAFKA
completed with exit 0. Maven downloaded dependencies and reported BUILD SUCCESS.
JUnit: 1 test, 0 failures, 0 errors, 0 skipped; test duration 12.01 seconds.
Report: tests/feasibility/finalizer-isolation/target/surefire-reports/fakegen.IsolationTest.txt.
Execution log: reports/isolation-probe.log. No secret values are included here.

Observed: one stream thread with two input partitions; partition 0 blocked in a
bounded callback, then a healthy record sent to partition 1. During the five-second
observation, committed output count was zero and neither input offset advanced.
After callback release, both distinct event values were observed with read_committed.
Normal cleanup completed without exception (temporary input/output topics deleted).

Interpretation: this negative control proves shared-thread blocking violates healthy
partition progress. Test success certifies that observation, NOT a working isolation
policy. It models local waiting; it does not inject an actual SQL exception or prove
general no-loss/no-duplication under crashes. Missing SLF4J binding disabled runtime
logging, so thread/task assignment detail and heartbeat evidence were not collected.

Still NOT RUN: supported candidate isolation, actual SQL/validation exceptions,
heartbeat continuity, repeated failures, rebalance/takeover and transaction-abort
isolation. T101 remains incomplete until those establish a supported policy or a
definitive supported-API limitation. Next proposed stage: instrument logging and test
a supported isolation candidate, with any capacity/topology change explicitly reviewed.

## Instrumented one-thread / two-thread comparison — 2026-11-23

Same Docker image and Kafka Streams 4.1.2; SLF4J Simple 1.7.36 now enables runtime
task/thread logs. `mvn -B test`: BUILD SUCCESS, exit 0; 2 tests, 0 failures,
0 errors, 0 skipped, 28.54 seconds. Log: `reports/isolation-two-threads.log`;
Surefire report: `tests/feasibility/finalizer-isolation/target/surefire-reports/fakegen.IsolationTest.txt`.
Maven cache is local/ignored at `reports/maven-cache`; credentials remain process-local.

Both tests warm up each input partition and verify actual callback thread names.
Partition 0 then blocks before forwarding, with a follower queued behind it; a healthy
record is sent to partition 1. During a five-second observation:

| Configuration | New committed output | Input offsets p0 / p1 | Follower processed |
|---|---|---|---|
| One thread, two tasks | none | 1 / 1 | no |
| Two threads, distinct callback threads verified | healthy exactly once | 1 / 2 | no |

After release, both tests observe blocked/follower/healthy exactly once each (counts,
not just a set); blocked precedes follower. Final committed input offsets are 3 / 2.
An additional two-second read detects extra copies within that observation window.
Warmup contributes one earlier record per partition. Runtime logs identify tasks 0_0
and 0_1; callback evidence records which thread handles each record. Broker-committed
offsets plus read_committed output are transactional outcome evidence, not an abort
or fencing test. Cleanup finished without error; temporary input/output topics deleted.
The latch is released before Streams resource shutdown even if an assertion fails.

Supported API boundary: the experiment uses the public DSL, Admin/consumer/producer
APIs and documented `num.stream.threads` / `exactly_once_v2` configuration; no private
assignment APIs or exception swallowing. Configuration reference:
https://kafka.apache.org/41/configuration/kafka-streams-configs/.
This is a candidate with verified assignment in one steady-state run, not a policy
guaranteeing one task per thread after membership/capacity changes. No production
configuration or architecture is adopted by this experiment.

Still **NOT RUN**: actual catalog/SQL or validation failures, heartbeat continuity
during prolonged retry, repeated-failure recovery, rebalance/takeover, transaction
abort isolation, state-store recovery and six-partition capacity/failover behavior.
Output partition mapping is not asserted by this probe. Partial topic-creation
failure cleanup remains untested. The experiment does not prove end-to-end exactly
once for the production SQL-to-final pipeline. T101 remains unchecked.

Next proposed approved scope: extend T101 with fault injection and controlled
rebalance/takeover, capturing healthy progress and failed-input offset invariants;
route any required capacity/topology changes through speckit-plan and analysis.
