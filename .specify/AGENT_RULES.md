# Shared Spec Kit rules

Applies to Codex and Claude now, and to future agent integrations.

- Use this checkout's `.specify/` templates, scripts and constitution, and `specs/` artifacts as the shared project source. Keep agent invocation files in their native directories and install adapters with `specify integration install`; keep Claude installed when adding another adapter.
- Before each Spec Kit shell invocation, set `SPECIFY_INIT_DIR` to the absolute checkout root, `SPECIFY_FEATURE_DIRECTORY` to the explicitly selected feature directory, and `SPECIFY_FEATURE_NO_PERSIST=1`. Set `SPECIFY_FEATURE` to that directory's basename. Desktop tool shells may be fresh processes: repeat these assignments in each call rather than relying on a previous call's environment. The CLI session launcher supplies them to child processes automatically.
- If the feature is unspecified, resolve it from the user's task; when ambiguous, ask before writing. For a new feature, set `SPECIFY_INIT_DIR` and `SPECIFY_FEATURE_NO_PERSIST=1`, run create-new-feature once, then use the returned SPEC_FILE parent directory explicitly. Serialize feature creation in a shared checkout because sequential number allocation is not locked.
- Concurrent edits use separate Git worktrees for independent branches. Each worktree must contain the required Spec Kit configuration and feature artifacts; uncommitted files in another checkout are not inherited. Preserve existing dirty work when preparing worktrees.
- In one shared checkout, agree on one writer per file before editing. One agent owns spec/plan/tasks generation and tasks.md status updates; others review read-only or edit explicitly assigned, disjoint implementation files. Serialize constitution, integration, template and workflow changes. A session-local feature pointer does not lock files or prevent Git branch conflicts.
- Hand off the feature path, task IDs, owned files, checks and outstanding decisions explicitly. Treat changes made by another agent as existing user work.
- Invoke Codex skills with `$speckit-<name>` and Claude skills with `/speckit-<name>`. Select the integration explicitly for automated workflows instead of changing the shared default during concurrent sessions. Use CLI help for the installed workflow's argument syntax.

Session commands and maintenance: `docs/speckit-multi-agent.md`.
