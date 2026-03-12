# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Update all relevant repo docs to reflect the fixed-target daily update behavior, delisted-symbol archive path, and repeatable DuckDB rebuild semantics, then commit and push the full recovery change set.

## Success Criteria
- Root operator docs, agent-facing guides, and architecture notes all reflect the live behavior for `--target-date`, delisted symbol archival, and DuckDB rebuild semantics.
- The doc sweep is validated against the code and the current repo state.
- The full test suite still passes after the runtime and doc updates.
- The resulting change set is committed and pushed to the current branch.

## Dependency Graph
- T1 -> T2
- T2 -> T3
- T3 -> T4

## Tasks
- [x] T1 Audit every relevant doc and identify the exact runtime facts that changed
  depends_on: []
- [x] T2 Update operator docs, agent-facing guides, and workflow lessons to match the live behavior
  depends_on: [T1]
- [x] T3 Verify the updated code and docs against the current repo state and rerun regression coverage
  depends_on: [T2]
- [x] T4 Commit the full recovery change set and push it to the current branch
  depends_on: [T3]

## Review
- Outcome:
  - Updated [`README.md`](/Users/joemccann/dev/apps/finance/market-data-warehouse/README.md), [`CLAUDE.md`](/Users/joemccann/dev/apps/finance/market-data-warehouse/CLAUDE.md), [`AGENTS.md`](/Users/joemccann/dev/apps/finance/market-data-warehouse/AGENTS.md), [.codex/project-memory.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/.codex/project-memory.md), [`docs/observability_defensive_blueprint.md`](/Users/joemccann/dev/apps/finance/market-data-warehouse/docs/observability_defensive_blueprint.md), and [`tasks/lessons.md`](/Users/joemccann/dev/apps/finance/market-data-warehouse/tasks/lessons.md) so they all document the fixed `--target-date` recovery mode, the `bronze-delisted` archive path for delisted symbols, and the scratch rebuild behavior for DuckDB.
  - The runtime change set remains the same as the recovery work: `daily_update.py` now supports fixed-date catch-up with a target-date cap, `DBClient.replace_equities_from_parquet()` now recreates the analytical tables on rebuild, `EB` was removed from future universe inputs and archived out of the canonical bronze tree, and the associated regression coverage was added.
- Verification:
  - `rg -n "target-date|bronze-delisted|recreates the analytical tables|delisted symbols|archive delisted" README.md CLAUDE.md AGENTS.md .codex/project-memory.md docs/observability_defensive_blueprint.md tasks/lessons.md`
  - `source ~/market-warehouse/.venv/bin/activate && python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing -W error::RuntimeWarning`
- Residual risk:
  - None beyond the unrelated dirty and untracked files already present in the worktree, which were intentionally left out of this commit.
