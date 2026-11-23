# v2 integration handoff — 2026-11-23

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
- Existing T101 evidence: 2 tests passed, 0 failed/errors/skipped, 28.54 seconds.
  Source is unchanged since that run; tests were not rerun just for Git integration.
- Extension Python/Bash execution and full functional adapter tests were not run.
  Host Python alias was unavailable; YAML parsing was not validated in this stage.
- Full .NET/SQL regression and production v2 acceptance were not run in this stage.

Review: the prototype only demonstrates bounded steady-state thread isolation.
It cannot establish no-loss/no-duplication across actual catalog errors, prolonged
retry, heartbeat failure, rebalance, takeover, abort or state-store recovery.
Partial topic-creation failure cleanup and output partition mapping are not tested.
These limits remain explicit in `docs/finalizer-isolation-feasibility.md`.
This branch is a checkpoint for continuing work, not a production acceptance gate.

## Next stage (requires user approval)

Continue the authorized workflow through `speckit-implement` for T101 fault injection
and controlled rebalance/takeover evidence. T101/U9 remain OPEN; do not mark them
complete based on the two passing steady-state tests. T064 dependency verification
is also still pending; the standalone feasibility probe does not complete it.
Return any capacity/topology changes through `speckit-plan` and `speckit-tasks`, then
run `speckit-analyze` before dependent finalizer implementation.
C1 reverse state/output audit remains a separately approved future correction.

At stage start, read `.specify/AGENT_RULES.md`. Before each Spec Kit shell invocation,
set SPECIFY_INIT_DIR to this checkout, SPECIFY_FEATURE_DIRECTORY to the feature above,
SPECIFY_FEATURE_NO_PERSIST=1 and SPECIFY_FEATURE=001-fake-data-kafka-publishing.
Keep one writer per shared artifact and preserve stable task IDs. Report stage results
and request approval before starting the following stage.
