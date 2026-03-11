# Current App Checklist

Use this checklist before making assistant UX recommendations for the macOS app.

## Product constraints

- Bronze parquet remains canonical storage.
- DuckDB CLI parity is a hard requirement.
- The assistant must not hide raw SQL or raw `duckdb` execution paths.
- The current provider path is local CLI execution with API-key fallback.

## Current implementation seams

- First-run gating happens above the shell in `MarketDataWarehouseApp`.
- Runtime state and persisted session hydration live in `OperatorPilotViewModel`.
- Session JSON persists through `AppSessionStore`.
- Provider API keys persist through `KeychainStore`.
- Setup and Settings both bind to the shared view model.

## UX checks

- Is the provider identity visible?
- Is the auth path visible?
- Can the user copy, edit, or rerun the exact command?
- Are failures actionable?
- Does the proposal preserve macOS Settings, menus, shortcuts, and accessibility?
- Does the proposal add AI only where it saves time instead of hiding control?
