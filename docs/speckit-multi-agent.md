# Spec Kit: Codex and Claude together

Spec Kit 1.4.0 uses shared `.specify/` infrastructure and `specs/` artifacts, with separate native adapters. Claude remains the default. Codex uses `.agents/skills/`; Claude uses `.claude/skills/`. Gemini is a future integration, not installed by this change.

## Start a CLI session

Run from the checkout root, in a separate terminal for each agent:

```powershell
& ./.specify/scripts/powershell/Start-SpecKitSession.ps1 -Agent codex -FeatureDirectory specs/001-fake-data-kafka-publishing
& ./.specify/scripts/powershell/Start-SpecKitSession.ps1 -Agent claude -FeatureDirectory specs/001-fake-data-kafka-publishing
```

Run only one of those commands per terminal. The launcher sets feature selection in the child process environment, prevents writes to `.specify/feature.json`, and restores the calling process environment afterward. `-CheckOnly` prints resolved paths without starting a model or modifying feature artifacts. `-AgentArguments @('...')` passes arguments to the chosen CLI.

In Codex invoke `$speckit-specify`, `$speckit-clarify`, `$speckit-plan`, `$speckit-tasks`, `$speckit-analyze`, `$speckit-implement`, `$speckit-checklist`, `$speckit-converge`, or `$speckit-constitution`. Claude uses `/speckit-...` for the corresponding skills. Restart/reopen a Codex session if newly installed skills are not listed.

## Codex desktop or an already running agent

The launcher does not change an already running desktop process. Include the absolute feature directory in the task. The root instructions route both agents to `.specify/AGENT_RULES.md`. Each shell invocation must include its own context, for example:

```powershell
$env:SPECIFY_INIT_DIR = (Get-Location).Path
$env:SPECIFY_FEATURE_DIRECTORY = Join-Path $env:SPECIFY_INIT_DIR 'specs/001-fake-data-kafka-publishing'
$env:SPECIFY_FEATURE = Split-Path $env:SPECIFY_FEATURE_DIRECTORY -Leaf
$env:SPECIFY_FEATURE_NO_PERSIST = '1'
& ./.specify/scripts/powershell/check-prerequisites.ps1 -Json -PathsOnly
```

## Create a feature

One writer creates the feature; other agents use its returned path:

```powershell
$env:SPECIFY_INIT_DIR = (Get-Location).Path
$env:SPECIFY_FEATURE_NO_PERSIST = '1'
& ./.specify/scripts/powershell/create-new-feature.ps1 -Json -ShortName 'example-feature' 'Describe the feature'
```

Use the returned SPEC_FILE parent directory in the launcher. Creation is serialized in a shared checkout; timestamp names alone do not guarantee collision freedom.

## Concurrent work

Read `.specify/AGENT_RULES.md` for ownership and handoff requirements. Separate worktrees provide independent branch/file state. This repository already has uncommitted configuration and implementation work: a new worktree from HEAD will not contain those changes. Transfer reviewed configuration/artifacts explicitly or create worktrees after the relevant changes are committed. Never reset the current work to prepare another agent.

For collaboration in this checkout, assign disjoint files and one owner for tasks.md. Two agents must not generate the same plan/spec/tasks concurrently. The launcher isolates feature selection; it is not a file-locking or merge service.

## Maintenance and adding agents

```powershell
specify integration status
specify integration install gemini --script ps
```

Gemini installation is a later step. After installation, add its native instruction entry point referring to the shared rules and validate its command discovery. Use additive `install` rather than `switch` when retaining existing agents. The launcher accepts Gemini once registered. Other agents can use the same process environment contract.

Avoid force-refreshing all integrations as a setup shortcut. The existing Claude skills have local modifications relative to their manifest; review them before upgrading. Generated Codex skills are owned by the official installer, while local collaboration instructions live outside generated skills. Inspect `specify workflow run --help` and choose Codex explicitly when dispatching automated workflows; otherwise the preserved default selects Claude.
