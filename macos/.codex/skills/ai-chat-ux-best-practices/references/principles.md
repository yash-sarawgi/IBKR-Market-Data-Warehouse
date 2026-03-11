# Assistant UX Principles

## Native macOS first

- Keep the shell native: split views, toolbars, menus, keyboard shortcuts, Settings scene, file panels, and accessibility semantics.
- Use in-content settings only when they mirror the same state as the dedicated macOS Settings scene.

## Trust surfaces

- Separate assistant prose from SQL previews, raw DuckDB previews, and executed command results.
- Show provider identity, auth mode, and recovery state.
- Preserve deterministic paths such as `/sql ...` and `/duckdb ...`.

## Setup and settings

- First-run setup should be short and blocking only on the first launch.
- Reconfigure later through Settings or a setup sheet, not a wizard maze.
- Explain missing CLI, missing login, and missing API key states distinctly.

## Prompting and evaluation

- Keep wrapper prompts structured and compact.
- Include the current source and only a short transcript tail.
- Prefer explicit commands over vague advice when the user is trying to manipulate local data.
- Evaluate prompt changes against representative data-work tasks before shipping.

## Persistence and security

- Keep ordinary session state in Application Support.
- Keep secrets in Keychain.
- Plan for security-scoped bookmarks if the app becomes sandboxed later.

## Primary Sources

- Apple HIG: Designing for macOS
- Apple HIG: Generative AI
- Apple HIG: File management
- Apple HIG: Search interface
- Apple HIG: Accessibility
- SwiftUI: Settings, NavigationSplitView, Table, AppStorage
- Foundation: bookmarkData
- OpenAI prompting guide
- OpenAI ChatKit
- Anthropic prompt engineering overview
- Google Gemini prompting strategies
