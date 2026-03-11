# Technical Foundation

These rules apply to all three design options.

## Platform Stack

- SwiftUI-first app shell targeting current macOS Tahoe-era APIs
- AppKit bridges only where SwiftUI does not provide the required fidelity
- Structured concurrency for query execution, auth refresh, and background indexing
- Swift package modularization so core logic is testable without UI harnesses

## Proposed Module Boundaries

- `AppShell`
  windows, navigation, menus, settings, scene restoration
- `WorkspaceCore`
  session state, query jobs, result models, diagnostics
- `DuckDBCLIAdapter`
  process orchestration, PTY transcript capture, parity tests against the `duckdb` binary
- `ParquetCatalog`
  filesystem discovery, previews, schema and partition metadata
- `ProviderAuth`
  subscription auth, Keychain storage, API-key fallback, provider status
- `ChatOrchestrator`
  message threading, tool-call plans, command previews, execution approvals

## DuckDB Parity Rule

The cleanest way to guarantee DuckDB CLI feature parity is to wrap the actual `duckdb` CLI binary instead of re-implementing shell semantics in Swift.

Recommended execution model:

- launch `duckdb` in a PTY-backed subprocess
- capture stdout, stderr, prompt state, exit codes, and interactive transcripts
- expose those transcripts directly in the app for debugging and copy/paste
- let the chat layer propose commands, but route execution through the same adapter as manual commands

Optional optimization:

- allow a read-only metadata helper path for fast schema previews, but treat it as derived convenience data
- never let that helper path diverge from the real execution semantics exposed to the user

## Data Rules

- Bronze parquet remains canonical storage.
- DuckDB remains an analytical view or execution engine over parquet and optional local `.duckdb` files.
- The first implementation should focus on safe exploration, query, preview, export, and diagnostics.
- Any future write path must preserve the repo's existing atomic publish guarantees and data-validation rules.

## Auth Resolution Strategy

Default user experience:

- try subscription-backed auth first when the user signs in through a provider adapter
- if the user explicitly enters an API key, honor it as an override
- if neither exists, allow environment-variable fallback for developer usage

Provider adapter responsibilities:

- detect installed local CLIs where that is a supported auth path
- support embedded native-app OAuth when the provider documents a public-client flow
- store refreshable credentials in Keychain
- expose a single diagnostic surface that shows auth source, expiry, refresh state, and last error

## Observability

- every DuckDB run should record timestamp, command text, working directory, data source, and exit status
- provider auth events should be logged with redaction-safe metadata only
- the app should export a diagnostics bundle for support and debugging
- logs should distinguish user-typed commands from model-suggested commands

## Coverage And Testing

The implementation target should be `100%` coverage for the macOS source set, backed by a design that makes coverage realistic instead of ceremonial.

Required test layers:

- unit tests for auth, command construction, transcript parsing, session reducers, and result models
- golden tests comparing app-launched DuckDB commands to direct CLI output
- integration tests for parquet discovery and database opening against temp fixtures
- UI tests for open file, run query, auth state changes, chat-to-command preview, and error recovery
- snapshot or accessibility tree checks for key navigation and inspector states

## Local Environment Observations

Detected on this machine during design work:

- `duckdb` at `/opt/homebrew/bin/duckdb`
- `claude` at `/Users/joemccann/.nvm/versions/node/v22.12.0/bin/claude`
- `codex` at `/Users/joemccann/.nvm/versions/node/v22.12.0/bin/codex`
- `gemini` at `/Users/joemccann/.nvm/versions/node/v22.12.0/bin/gemini`

Observed auth-related CLI surfaces:

- `claude --help` exposes `auth` and `setup-token`
- `codex --help` exposes `login` and `logout`
- `gemini` is installed locally and the current app build routes Gemini chat through the local CLI path

Current implementation note:

- the live macOS build currently uses provider CLIs as the primary assistant path, with Keychain-backed API-key fallback
- embedded native OAuth remains an optional future extension, not the only auth design
