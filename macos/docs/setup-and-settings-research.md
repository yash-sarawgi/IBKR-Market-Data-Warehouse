# Setup And Settings Research

This note captures the current insertion points and persistence seams for first-run setup and persistent settings in the live `macos/` package.

## Current State Ownership

- `MarketDataWarehouseApp` owns a single `OperatorPilotViewModel` for the app window.
- `OperatorPilotViewModel` owns the runtime state for:
  - `settings`
  - `sources`
  - `selectedSource`
  - `transcript`
  - `lastExecution`
  - `providerStatuses`
  - setup and diagnostics presentation flags
- `SetupFlowView` reads and writes setup state through the shared view model.
- `SettingsPaneView` uses the same shared view model in both the workspace and the dedicated macOS `Settings` scene.

## First-Run Gating

The current first-run gate is in the right place:

- `MarketDataWarehouseApp` decides whether to show `SetupFlowView` or `OperatorPilotRootView`.
- `OperatorPilotViewModel.requiresInitialSetup` drives that decision.
- After setup, reruns happen as a sheet from inside the main shell.

This matches native macOS expectations better than treating setup as just another sidebar tab.

Recommended rule:

- Keep initial setup as a blocking full-window path.
- Keep rerun setup as a sheet or the Settings scene.
- Keep the `Setup` sidebar destination as a summary and recovery surface, not the primary gate.

## Persistence Seams That Already Exist

### Application Support session store

`AppSessionStore` persists a JSON `AppSessionSnapshot` under:

```text
~/Library/Application Support/MarketDataWarehouseMac/session.json
```

That snapshot currently stores:

- `AppSettings`
- imported `DataSource` values
- selected source ID
- transcript history

There is also an environment override:

```text
MARKET_DATA_WAREHOUSE_SESSION_FILE
```

That is useful for smoke tests and isolated local sessions.

### Keychain secret store

`KeychainStore` persists provider API keys in the macOS Keychain and uses:

- generic password items
- per-provider account names
- `kSecAttrAccessibleWhenUnlockedThisDeviceOnly`

This is the correct seam for secret material. API keys should stay out of the JSON session snapshot.

## Persistence Gaps That Still Matter

### 1. Security-scoped bookmarks

The app currently persists raw file URLs for sources. That is fine for the current non-sandboxed local build, but it is not the correct long-term seam for a sandboxed Mac app.

If the app moves toward sandboxing or Mac App Store distribution, imported sources should switch from plain file URLs to security-scoped bookmarks.

### 2. Native OAuth token storage

The current shipping path uses local provider CLIs plus Keychain-backed API-key fallback. If the app adds native OAuth later, refresh tokens should get a separate Keychain-backed credential store instead of being mixed into `KeychainStore`.

### 3. Window and scene restoration

Settings and session content persist today, but richer window restoration is still a separate concern. If multi-window support is added, scene restoration should track:

- active destination
- window-specific selected source
- pending setup or diagnostics state

### 4. Retention policy

Transcript history persists indefinitely in the session snapshot. That is acceptable for the current local-first build, but future product work should decide:

- whether transcripts expire
- whether diagnostics bundles can be exported separately
- whether provider conversations and command transcripts should be retained differently

## Best-Practice Guidance For This App

### Setup flow

- Keep setup short: provider, auth mode, optional model, optional API key.
- Show real provider readiness during setup.
- Distinguish clearly between:
  - CLI installed but not logged in
  - CLI missing
  - API key available
  - API key missing
- Use Settings and setup summaries for ongoing maintenance, not just onboarding.

### Settings

- Treat the dedicated macOS Settings scene as the canonical preferences surface.
- Keep any in-shell settings view bound to the same source of truth.
- Keep task-oriented actions like `Run Setup Again`, `Refresh Provider Status`, and `Clear Conversation` close to the settings controls that explain them.

### Storage

- Continue using Application Support for session JSON.
- Continue using Keychain for secrets.
- Do not move secrets into `UserDefaults`, plist files, or the session snapshot.
- If sandboxing becomes a goal, prioritize bookmark migration before expanding persisted source features.

## Live Code Touchpoints

- `Sources/MarketDataWarehouseApp/MarketDataWarehouseApp.swift`
- `Sources/OperatorPilotKit/OperatorPilotViewModel.swift`
- `Sources/OperatorPilotKit/SetupFlowView.swift`
- `Sources/OperatorPilotKit/AppSessionStore.swift`
- `Sources/OperatorPilotKit/KeychainStore.swift`
- `Sources/MarketDataCore/AppSettings.swift`
