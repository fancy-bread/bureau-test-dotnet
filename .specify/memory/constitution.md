<!--
SYNC IMPACT REPORT
Version change: 1.0.0 → 1.0.1
Modified principles: none
Added sections: Quality Gates — Phase Checkpoints (gate 5)
Removed sections: none
Templates requiring updates:
  ✅ .specify/memory/constitution.md — updated
  ✅ specs/001-kafka-observability-dashboard/tasks.md — commit tasks added at end of each phase
Follow-up TODOs: none
-->

# Bureau Test Harness Constitution

## Core Principles

### I. Specs as Test Cases

Every spec in this repo is an e2e test case for the Bureau CLI. Each spec MUST be independently runnable by bureau — `bureau run <spec> --repo ./` MUST complete without relying on state from a previous spec run. Specs are numbered sequentially (`001-`, `002-`, etc.) and each MUST have acceptance criteria that are checkable by bureau's Critic persona without human intervention.

### II. Test-First (NON-NEGOTIABLE)

TDD is mandatory. Tests MUST be written before implementation code. The red-green-refactor cycle is strictly enforced: tests fail first, then implementation is written to make them pass. No implementation file may be committed without a corresponding test file. Bureau's Critic persona MUST verify this sequence was followed.

### III. C#/.NET as Primary Implementation Language

All implementation code in this repo is C# targeting .NET. Tooling and configuration specs (CI workflows, `.editorconfig`, NuGet config) produce YAML/JSON/config files — not C# code — and are explicitly scoped as such in their spec. Mixed-language specs are not permitted; each spec targets one output type.

### IV. Minimal Scope

No feature, abstraction, or dependency beyond what the spec requires. Bureau MUST NOT add error handling for impossible cases, future-proof code, or introduce helpers not called for by the spec. Scope creep is a Critic violation. If a spec is ambiguous about scope, bureau MUST escalate rather than expand.

### V. Verifiable Outputs

Every spec MUST define at least one acceptance scenario that can be verified by running a command (e.g., `dotnet test`, `dotnet build`, `dotnet run --project <project>`). Specs that produce only prose, documentation, or unrunnable artifacts are out of scope for this harness.

## Spec Test Contract

Each spec in `specs/` represents a contract between the spec author and bureau:

- The spec author guarantees: a valid spec with no `[NEEDS CLARIFICATION]` markers, a target state that is achievable from the current repo state, and runnable acceptance criteria.
- Bureau guarantees: implementation that satisfies acceptance scenarios, passing tests, no constitution violations, and a PR with a structured run summary.

Specs are cumulative — later specs may build on artifacts produced by earlier ones. Spec ordering (`001`, `002`, …) reflects dependency order.

## Quality Gates

Bureau's run against any spec in this repo MUST pass all of the following before a PR is opened:

1. **Tests pass** — `dotnet test` exits 0
2. **No constitution violations** — Critic persona finds no CRITICAL findings
3. **Acceptance scenarios verified** — each scenario from the spec is explicitly checked by the Critic
4. **Minimal scope confirmed** — no files modified outside the spec's declared scope
5. **Phase checkpoints committed** — each spec phase ends with a Reviewer review and a commit before the next phase begins. Setup and Foundational phases require `dotnet build` to exit 0. User Story phases and later require `dotnet test` to exit 0. A phase MUST NOT be committed if its gate fails.

Failure at any gate MUST produce a structured escalation, not a PR.

## Governance

This constitution supersedes all other practices in this repo. Amendments require: a description of what changed and why, a version bump per semantic versioning, and ratification date update. All PRs produced by bureau are reviewed against this constitution before merge. Constitution violations discovered post-merge are tracked as bugs against the bureau CLI, not against this repo.

**Version**: 1.0.1 | **Ratified**: 2026-05-01 | **Last Amended**: 2026-05-02
