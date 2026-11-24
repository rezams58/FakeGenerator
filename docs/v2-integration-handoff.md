# v2 integration handoff — updated 2026-11-24

Integration branch: `codex/001-v2-integration`.
Feature: `specs/001-fake-data-kafka-publishing`.
Base: `6c8e00a` (`main`, matching `origin/main` at integration).

## Included work

Topic branches were built in dependency order and merged explicitly into the
integration branch with merge commits:

| Topic branch | Commits | Scope |
|---|---|---|
| `codex/001-agent-governance` | `53ebc16` | Shared Spec Kit workflow and stage approval rules |
| `codex/001-speckit-extensions` | `17b3cc0` | Preserve installed Git, agent-context and bug extensions and Claude adapters |
| `codex/001-v2-design` | `f01f18e`, `954ecfa` | v2 requirements, constitution, contracts, plan and design corrections |
| `codex/001-t101-isolation` | `d613a73` | v2 task status, isolation prototype and measured evidence |

The extension commit preserves existing installed content; it is not a claim that
every extension command has been functionally tested on every agent. Shared sources
remain under `.specify/`; existing Codex and Claude core skill entry points remain.
No production C# implementation or SQL migration is changed in this integration.

## Validation and review limits

- Shared Spec Kit prerequisite resolution succeeds with explicit feature environment.
- `git diff --check` passes; integration content equals the topic-branch tip before
  this handoff file. All four topic heads are included in integration history.
- Five extension PowerShell scripts parse; registry and defaults JSON parse.
- A targeted secret-pattern scan found no matches. Ignored local.env, Maven cache,
  broker logs and build outputs are excluded from commits.
- Latest T101 evidence: 3 tests passed, 0 failures/errors/skipped, 54.48 seconds.
  Includes injected validation failure, client shutdown and replacement takeover.
  Initial attempt had one takeover timeout failure; the test session/heartbeat
  settings were made explicit (10s/3s) before the successful rerun. Both runs and
  limitations are recorded in docs/finalizer-isolation-feasibility.md.
  Tests were not rerun solely for session handoff.
- Extension Python/Bash execution and full functional adapter tests were not run.
  Host Python alias was unavailable; YAML parsing was not validated in this stage.
- Full .NET/SQL regression and production v2 acceptance were not run in this stage.

Review: the prototype demonstrates bounded steady-state thread isolation and
sequential replacement replay after an injected validation exception. Failed input
offset remained 1; shutdown also stopped healthy progress. Replacement observed
each expected record once and final input offsets were 3/3. SHUTDOWN_CLIENT is a
rejected independence policy. This does not prove live rebalance, actual SQL faults,
prolonged retry, heartbeat continuity, zombie fencing or persistent-state recovery.
Partial topic-creation failure cleanup and output partition mapping are not tested.
These limits remain explicit in `docs/finalizer-isolation-feasibility.md`.
This branch is a checkpoint for continuing work, not a production acceptance gate.

## Next stage (requires user approval)

Proposed next workflow: `speckit-plan` to evaluate a supported partition-isolation
policy using the new negative evidence, subject to explicit user stage approval.
Do not adopt extra processes/topics or capacity changes without architecture approval.
T101/U9 remain OPEN; do not mark them complete based on passing feasibility tests.
T064 dependency verification is still pending; the standalone probe does not complete it.
Return any capacity/topology changes through `speckit-plan` and `speckit-tasks`, then
run `speckit-analyze` before dependent finalizer implementation.
C1 reverse state/output audit remains a separately approved future correction.

User asked why Java was used instead of .NET. The approved design targets Kafka
Streams JVM EOS-v2 for atomic state/output/input-offset commits. Existing generator,
SQL and Outbox remain .NET. Java is not a user-mandated language; explain alternatives
and costs if revisiting the architecture, and do not treat current feasibility tests
as evidence that Kafka Streams already meets every requirement.

## Resume in a new session

Workspace: D:/WorkSpace/Ideas/FakeGenerator_src. Use the integration branch above.
Read this file, shared agent rules, then the current task/plan/processing contracts
and feasibility report in bounded sections. Do not restart completed v1 work.
The last approved scope was session handoff and committing the latest T101 evidence;
the next architecture stage still requires approval. Main is not the integration target.
The five original topic/integration branches were pushed successfully before the latest
T101 edits; verify remote status before assuming the handoff commit is published.
Local test configuration is ignored at tests/FakeGenerator.Tests/local.env. Never print
credentials. Java/Maven tests run in maven:3.9.9-eclipse-temurin-17 via Docker, with the
probe mounted at /work and reports/maven-cache mounted at /root/.m2. Logs and target/
are ignored; committed reports contain the portable evidence and limitations.

At stage start, read `.specify/AGENT_RULES.md`. Before each Spec Kit shell invocation,
set SPECIFY_INIT_DIR to this checkout, SPECIFY_FEATURE_DIRECTORY to the feature above,
SPECIFY_FEATURE_NO_PERSIST=1 and SPECIFY_FEATURE=001-fake-data-kafka-publishing.
Keep one writer per shared artifact and preserve stable task IDs. Report stage results
and request approval before starting the following stage.
