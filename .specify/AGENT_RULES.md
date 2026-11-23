# Shared Spec Kit rules

Applies to Codex and Claude now, and to future agent integrations.

## Spec Kit first

- At the start of every stage, identify the applicable Spec Kit workflow and use its installed skill, shared template and prerequisite checks before changing artifacts or implementation. State the selected workflow to the user.
- Route requirements to `speckit-specify`, unresolved requirements to `speckit-clarify`, architecture to `speckit-plan`, task generation to `speckit-tasks`, consistency review to `speckit-analyze`, implementation to `speckit-implement`, and governance amendments to `speckit-constitution`. Use checklist/converge when their scope applies. Select the workflow appropriate to the stage; do not rerun completed stages without cause.
- Requirements, plan, contracts and tasks must agree before implementation. After a material requirement/architecture change, run `speckit-analyze` before `speckit-implement`; resolve blocking findings through the corresponding workflow. A standalone design note does not replace Spec Kit artifacts.
- Record implementation and validation against task IDs in shared `tasks.md`. If a workflow is unavailable or blocked, report the limitation and proposed fallback before substituting a different process.

## Agent-independent changes

- Keep authoritative requirements, workflow rules, decisions, task status and evidence in shared repository files. Agent-specific entry points are thin adapters to the same sources, never separate copies of project policy.
- Every change must preserve the shared contracts used by Claude, Codex and future integrations: paths, environment variables, task IDs and handoff metadata. Avoid relying on one agent's private memory, tool name or local configuration for correctness.
- When modifying workflows or integrations, validate the installed agents' entry points and shared prerequisite resolution. Report which adapters were checked; future or unavailable agents are not claimed tested. Add future integrations through the existing adapter contract without replacing installed integrations.

## Stage completion and approval

After each user-approved stage, report results and checks, name the next stage and its Spec Kit workflow, and request explicit approval before starting it. Wait for approval. Authorization covers necessary implementation and verification within that stage; explicitly authorized multi-stage work may be completed together.

- Use this checkout's `.specify/` templates, scripts and constitution, and `specs/` artifacts as the shared project source. Keep agent invocation files in their native directories and install adapters with `specify integration install`; keep Claude installed when adding another adapter.
- Before each Spec Kit shell invocation, set `SPECIFY_INIT_DIR` to the absolute checkout root, `SPECIFY_FEATURE_DIRECTORY` to the explicitly selected feature directory, and `SPECIFY_FEATURE_NO_PERSIST=1`. Set `SPECIFY_FEATURE` to that directory's basename. Desktop tool shells may be fresh processes: repeat these assignments in each call rather than relying on a previous call's environment. The CLI session launcher supplies them to child processes automatically.
- If the feature is unspecified, resolve it from the user's task; when ambiguous, ask before writing. For a new feature, set `SPECIFY_INIT_DIR` and `SPECIFY_FEATURE_NO_PERSIST=1`, run create-new-feature once, then use the returned SPEC_FILE parent directory explicitly. Serialize feature creation in a shared checkout because sequential number allocation is not locked.
- Concurrent edits use separate Git worktrees for independent branches. Each worktree must contain the required Spec Kit configuration and feature artifacts; uncommitted files in another checkout are not inherited. Preserve existing dirty work when preparing worktrees.
- In one shared checkout, agree on one writer per file before editing. One agent owns spec/plan/tasks generation and tasks.md status updates; others review read-only or edit explicitly assigned, disjoint implementation files. Serialize constitution, integration, template and workflow changes. A session-local feature pointer does not lock files or prevent Git branch conflicts.
- Hand off the feature path, task IDs, owned files, checks and outstanding decisions explicitly. Treat changes made by another agent as existing user work.
- Invoke Codex skills with `$speckit-<name>` and Claude skills with `/speckit-<name>`. Select the integration explicitly for automated workflows instead of changing the shared default during concurrent sessions. Use CLI help for the installed workflow's argument syntax.

Session commands and maintenance: `docs/speckit-multi-agent.md`.
