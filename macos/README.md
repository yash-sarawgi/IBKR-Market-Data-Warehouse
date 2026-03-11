# macOS App Design Package

This directory now contains the runnable native macOS client for this repo, plus the earlier research, design, and render artifacts that led to it.

## Scope

- Native macOS app for exploring parquet and DuckDB data
- Chat-first workflow with direct DuckDB command execution
- DuckDB CLI feature parity as a hard requirement
- Local-provider subscription workflow through installed CLIs, with API-key fallback
- Full-code-coverage expectation carried into the future implementation plan
- Selected direction: `Option 3: Operator Pilot`

## Current App Features

- First-run setup launches before the main workspace until a default provider is configured.
- Settings are available both in-app and through the standard macOS Settings scene.
- Provider-backed chat routes through the selected local `claude`, `codex`, or `gemini` CLI.
- API keys can be stored per provider in Keychain and are used as a fallback when local subscription auth is unavailable.
- Session state persists locally, including settings, transcript history, imported sources, and the selected source.
- Raw DuckDB CLI passthrough is available from chat with `/duckdb ...` and from the diagnostics drawer.
- The workspace now uses a hybrid SwiftUI plus MetalKit architecture, with Metal-backed status panels rendered through `MTKView`.
- Standard macOS commands are wired for opening sources and rerunning setup without leaving the app shell.
- Source-aware prompt planning still supports direct SQL execution for local `.parquet`, `.duckdb`, and `.db` files.

## Contents

- `Package.swift`
  repo-local Swift package that Xcode 26.3 can open directly
- `Sources/`
  the runnable Option 3 app shell and support modules
- `Tests/`
  unit coverage for planner, CLI adapter, provider chat, session persistence, and view-model seams
- `docs/macos-ui-research.md`
  Apple-source UI and UX guidance for current macOS design patterns
- `docs/technical-foundation.md`
  Shared architecture and testing guardrails that all three designs depend on
- `docs/subscription-oauth-debug-reference.md`
  Working auth reference captured from the user-provided OAuth flow plus local CLI observations
- `docs/metal-replatform.md`
  Repo-specific Metal architecture, build rules, and downloaded references
- `docs/ai-chat-ux-best-practices.html`
  HTML research report for assistant UX, setup, settings, trust surfaces, and prompt-operation rules
- `docs/setup-and-settings-research.md`
  Current-package note covering first-run gating, settings ownership, persistence seams, and future bookmark/OAuth considerations
- `docs/metal-best-practices.md`
  Condensed Apple-source guidance for the current hybrid SwiftUI plus MetalKit approach
- `docs/design-comparison.md`
  Comparison matrix and recommendation across the three concepts
- `designs/option-1-navigator-console.md`
  Explorer-first layout with a raw command lane
- `designs/option-2-workbench-canvas.md`
  Analysis-canvas layout for richer comparative workflows
- `designs/option-3-operator-pilot.md`
  Chat-first operator surface closest to the provided reference images
- `.codex/skills/ai-chat-ux-best-practices/`
  Repo-local Codex skill for future assistant UX audits and refinements inside `macos/`

## Build And Test

```bash
cd macos
swift build
swift test
./scripts/run_ui_smoke_tests.sh
```

`run_ui_smoke_tests.sh` launches an isolated app session, drives the UI with keyboard shortcuts, and verifies visible states with OCR on the app window. The smoke flow covers first-run setup, navigation, rerunning setup from Settings, diagnostics, raw DuckDB CLI execution, provider-backed chat, source import, and parquet preview.

## Metal Replatform

The live app now uses a hybrid rendering model:

- SwiftUI for the desktop shell, forms, transcript text, menus, and settings controls
- MetalKit for the dense visual status surfaces embedded in the workspace

That architecture is deliberate. For a macOS productivity app, a permanent full-screen game-style renderer is usually the wrong tradeoff. The current implementation keeps `MTKView` paused until state changes, then temporarily unpauses it only while work is actively running.

References for the current Metal work:

- `docs/metal-replatform.md`
- `docs/metal-best-practices.md`
- `vendor/apple/Metal-Feature-Set-Tables.pdf`
- `vendor/metal-guide/README.md`

If the Metal compiler is not available locally, install the optional Xcode component once:

```bash
xcodebuild -downloadComponent metalToolchain
```

The app-bundle build script now precompiles `OperatorPilotMetalShaders.metallib` with:

- `scripts/compile_metal_library.sh`
- `scripts/build_local_macos_app.sh`

To open the app bundle that Finder and the launcher use:

```bash
cd macos
./scripts/build_local_macos_app.sh
open "build/Market Data Warehouse.app"
```

## Keyboard Commands

- `Cmd-O` open a parquet, DuckDB, or SQLite source
- `Cmd-Shift-R` rerun setup
- `Cmd-Shift-D` toggle the diagnostics drawer
- `Cmd-L` focus the composer
- `Cmd-1` switch to Assistant
- `Cmd-2` switch to Transcripts
- `Cmd-3` switch to Setup
- `Cmd-4` switch to Settings

## Finder Launcher

To avoid Terminal for local testing, this directory now includes a Finder-friendly launcher flow:

- Source AppleScript: `launcher/Launch Market Data Warehouse.applescript`
- Bundle build script: `scripts/build_local_macos_app.sh`
- Generator script: `scripts/build_local_launcher.sh`
- Build-and-open script: `scripts/build_and_launch_local_app.sh`
- Generated local macOS app build: `build/Market Data Warehouse.app`
- Generated local app: `launcher/Launch Market Data Warehouse.app`

To build the local macOS app bundle directly:

```bash
cd macos
./scripts/build_local_macos_app.sh
```

That creates:

```text
macos/build/Market Data Warehouse.app
```

To generate the double-clickable launcher once:

```bash
cd macos
./scripts/build_local_launcher.sh
```

After that, double-click `launcher/Launch Market Data Warehouse.app` in Finder. It will:

- build `build/Market Data Warehouse.app`
- open the generated app bundle
- avoid opening Terminal

If the build or launch fails, check `macos/logs/build-and-launch.log`.

## Shared Product Rules

- Bronze parquet remains the canonical storage layer.
- DuckDB remains the analytical/query engine, not a competing source of truth.
- The app should expose the raw DuckDB command and output path, not only AI-generated abstractions.
- Provider secrets and refresh tokens must live in Keychain, never in plist or flat files.
- Initial implementation should favor read-only exploration until explicit write workflows are specified.
