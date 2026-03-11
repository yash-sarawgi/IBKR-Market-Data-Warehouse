---
name: ai-chat-ux-best-practices
description: Use this skill when planning, auditing, or refining the assistant UX in the native macOS client, especially setup, settings, chat trust surfaces, prompt wrappers, and command transparency.
---

# AI Chat UX Best Practices

## Overview

Use this skill for assistant UX work in `macos/` when the product decision touches chat ergonomics, provider selection, first-run setup, settings, prompt wrappers, transcript behavior, or trust around executed DuckDB commands.

This app is a local data workstation, not a generic chatbot. The assistant must accelerate parquet and DuckDB workflows without obscuring the raw SQL and DuckDB path.

## Workflow

1. Start from product constraints.
   Read `references/current-app-checklist.md` to anchor the work in this repo's actual requirements: local-first data access, DuckDB CLI parity, local provider CLI auth, API-key fallback, Keychain secrets, and persisted local sessions.
2. Read the design principles.
   Read `references/principles.md` before proposing UI or behavior changes. The key themes are native macOS semantics, explicit trust surfaces, progressive setup, and deterministic escape hatches.
3. Audit the full assistant journey.
   Evaluate the request across first-run setup, settings, provider status, prompt entry, assistant response, SQL or raw-command preview, execution result, error recovery, and diagnostics.
4. Keep native macOS behavior intact.
   Prefer the standard Settings scene, `NavigationSplitView`, menus, keyboard shortcuts, file panels, `Table`, and accessibility-first controls over custom web-style patterns.
5. Preserve command transparency.
   Assistant prose, SQL previews, raw `duckdb` previews, and executed results must stay visibly separate. Users should always be able to copy, edit, rerun, or bypass AI-generated suggestions.
6. Treat prompts as versioned product behavior.
   When wrapper prompts or provider behavior change, update regression fixtures or UI smoke coverage where it fits.

## Rules

- Always show which provider produced the answer.
- Make the active auth path legible: local CLI session vs API key fallback.
- Keep setup short and recoverable. The first run should block once; later reconfiguration should happen through Settings or a sheet.
- Store secrets in Keychain and ordinary session state in Application Support.
- If future sandboxing is introduced, migrate persisted source access from plain file URLs to security-scoped bookmarks.
- Keep assistant copy concise and operational. If SQL or a DuckDB command is the right answer, prefer that over vague prose.
- Add or update tests when prompt wrappers, recovery behavior, or trust surfaces change materially.

## References

- Read `references/principles.md` for the condensed external research.
- Read `references/current-app-checklist.md` for repo-specific implementation guardrails.
- Read `../../../docs/ai-chat-ux-best-practices.html` for the full research report.
- Read `../../../docs/setup-and-settings-research.md` for the current app-state and persistence seams.
