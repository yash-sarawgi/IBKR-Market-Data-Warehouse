# Native macOS UI Research

## Platform Baseline

As of March 10, 2026, Apple's current public macOS release family is `macOS Tahoe 26.x`, with Apple support pages already covering `macOS Tahoe 26.3`. The UI work for this app should therefore target the current Human Interface Guidelines and SwiftUI/AppKit patterns for Tahoe-era macOS, not older Big Sur or Ventura conventions.

## What The App Should Feel Like

The reference images point toward a quiet, graphite-dark workspace with:

- a full-height sidebar
- a restrained unified toolbar
- a large uninterrupted work area
- subtle separation instead of heavy card chrome
- a bottom-aligned chat composer

The native interpretation of that style should still lean on macOS semantics instead of custom web-like chrome. That means using system materials, semantic colors, SF Symbols, native focus rings, standard sidebar behavior, and native keyboard/menu integration.

## Native Components To Use

### 1. Window and scene model

- Use a standard macOS main window with restored state and resizable split views.
- Support multi-window workflows for separate sessions or connections if users open multiple datasets.
- Keep Settings in a dedicated Settings scene as the canonical preferences surface. If the app mirrors settings in-content, both surfaces should bind to the same state and not diverge.

### 2. Navigation shell

- Use `NavigationSplitView` as the main shell.
- The leading sidebar should own navigation between parquet browsing, DuckDB connections, saved sessions, and diagnostics.
- Preserve native sidebar collapse/expand behavior and keyboard shortcuts.

### 3. Data presentation

- Use `Table` for DuckDB query results, parquet previews, schema browser rows, and logs that benefit from column sorting and resizing.
- Use lists or outline-style navigation for file trees, query history, and parquet partitions.
- Support column visibility, sorting, copy, export, and contextual menus.

### 4. Search and command access

- Use native searchable patterns for file, table, symbol, and session search.
- Add a command palette or quick action sheet for frequent actions like open file, run query, reconnect provider, inspect schema, and export results.
- Mirror all major actions in the app menus and keyboard shortcuts.

### 5. Inspector and secondary detail

- Use a trailing inspector for schema metadata, parquet statistics, model/provider settings, query plan details, and command diagnostics.
- Keep the inspector optional and collapsible so the main canvas remains quiet.

### 6. Toolbars and chrome

- Keep the toolbar sparse: source picker, active workspace name, run/cancel, search, provider status, and layout toggles.
- Use segmented controls and toolbar items rather than inventing custom tab bars.
- Surface background activity, sync, and auth state as subtle status indicators rather than persistent banners.

### 7. File and data access

- Use native file import/open panels for `.parquet`, `.duckdb`, and directory roots.
- Support drag-and-drop from Finder into the sidebar or content area.
- Use Quick Look where it helps with file inspection and exported artifacts.

### 8. Transient UI

- Use sheets or panels for one-off tasks such as adding a data root, connecting a new database, or completing OAuth login.
- Use alerts only for destructive or high-risk operations.
- Use inline error presentation for recoverable issues such as a failed query or missing provider login.

### 9. AI-specific UI rules

- Separate model-generated content from executed system actions.
- Always show the exact SQL or DuckDB command before or alongside execution.
- Mark provider identity, model, and authentication source clearly.
- Let users retry, edit, copy, and rerun AI-generated commands without hidden state.

### 10. Accessibility and keyboard support

- Everything must be reachable without a pointer.
- Respect VoiceOver labeling, high-contrast settings, Reduce Motion, Dynamic Type where applicable, and standard focus navigation.
- Menus, tables, chat transcript rows, and inspectors should all expose clear accessibility labels and actions.

## Visual Direction

### Recommended styling

- Default to a dark graphite appearance that still honors macOS vibrancy and semantic color tokens.
- Use large quiet surfaces, hairline separators, and restrained elevation.
- Prefer SF Pro and SF Mono for primary typography and code/command output.
- Use SF Symbols for navigation, status, and data-source affordances.

### What to avoid

- Browser-like tab strips that fight the macOS toolbar
- Over-designed cards everywhere
- Hidden actions that exist only behind hover or chat inference
- A custom settings system that drifts away from the native Settings scene
- Re-creating a faux terminal when a real command transcript is available

## Sources

- Apple support: `What's new in the updates for macOS Tahoe`
  https://support.apple.com/en-ie/guide/mac-help/mchl1fd68d8a/mac
- Apple support: `Download and install current or previous versions of the Mac operating system`
  https://support.apple.com/en-ie/102662
- Apple HIG: `Designing for macOS`
  https://developer.apple.com/design/human-interface-guidelines/designing-for-macos
- Apple HIG: `Menus`
  https://developer.apple.com/design/human-interface-guidelines/menus
- Apple HIG: `File management`
  https://developer.apple.com/design/human-interface-guidelines/file-management
- Apple HIG: `Search interface`
  https://developer.apple.com/design/human-interface-guidelines/search-interface
- Apple HIG: `Accessibility`
  https://developer.apple.com/design/human-interface-guidelines/accessibility
- Apple HIG: `Generative AI`
  https://developer.apple.com/design/human-interface-guidelines/generative-ai
- Apple Developer Documentation: `NavigationSplitView`
  https://developer.apple.com/documentation/swiftui/navigationsplitview
- Apple Developer Documentation: `Table`
  https://developer.apple.com/documentation/swiftui/table
