# Subscription OAuth Debug Reference

This document captures the user-provided OAuth subscription flow from March 10, 2026 so the macOS app has a repo-local debugging reference for any future native OAuth work.

Important:

- treat the provider-specific URLs, client IDs, scopes, and callback mechanics below as a working implementation reference, not a final verified contract
- verify the exact vendor behavior at implementation time against current provider docs and local CLI behavior
- the current app build primarily uses local provider CLIs plus API-key fallback, not embedded native OAuth

## Goal

Authenticate a native macOS app against a user's existing LLM subscription instead of requiring API billing by default.

## Core Flow

1. Generate a PKCE verifier and challenge for each login attempt.
2. Build the provider authorization URL with `response_type=code`, public `client_id`, `redirect_uri`, requested `scope`, PKCE challenge, and `state`.
3. Open the system browser and let the user approve access.
4. Capture the authorization code through either:
   - localhost callback server
   - custom URL scheme
   - manual code paste
5. Exchange `code + code_verifier` for `access_token + refresh_token`.
6. Store the credentials in Keychain.
7. Use the access token for inference and auto-refresh before expiry.

## Provider Table From The User Reference

| Provider | Subscription Tier | Authorize URL | Token URL | Suggested Scopes |
| --- | --- | --- | --- | --- |
| Anthropic | Claude Pro / Max | `https://claude.ai/oauth/authorize` | `https://console.anthropic.com/v1/oauth/token` | `org:create_api_key user:profile user:inference` |
| OpenAI | ChatGPT Plus / Pro | `https://auth.openai.com/oauth/authorize` | `https://auth.openai.com/oauth/token` | `openid profile email offline_access` |
| Google Gemini | Gemini subscription | Google OAuth authorize endpoint | Google token endpoint | Google-standard scopes |
| GitHub Copilot | Copilot Individual / Business | GitHub OAuth authorize endpoint | GitHub token endpoint | Copilot-specific scopes |

## Provider-Specific Details Captured From The User Reference

### Anthropic

- Public client ID: `9d1c250a-e61b-44d9-88ed-5944d1962f5e`
- Redirect URI: `https://console.anthropic.com/oauth/code/callback`
- Extra auth parameter: `code=true`
- Token endpoint content type: JSON
- Auth code delivery: manual paste
- Inference target: `https://api.anthropic.com/v1/messages`
- Auth header note from the user reference: Anthropic inference may expect the access token in `x-api-key` rather than `Authorization`

### OpenAI

- Public client ID: `app_EMoamEEZ73f0CkXaXp7hrann`
- Redirect URI: `http://localhost:1455/auth/callback`
- Extra auth parameters:
  - `id_token_add_organizations=true`
  - `codex_cli_simplified_flow=true`
- Token endpoint content type: form-url-encoded
- Auth code delivery: local callback server on port `1455`
- Inference target: `https://api.openai.com/v1/chat/completions`

## PKCE Notes

The reference flow uses OAuth 2.0 with PKCE because a native macOS app is a public client and cannot safely embed a client secret.

Implementation notes:

- generate a random verifier per login
- derive the challenge via SHA-256
- send `code_challenge_method=S256`
- persist `state` and validate it on callback

## Callback Strategies

### Localhost callback

Best fit for providers that redirect to `http://localhost:<port>/...`.

Requirements:

- start the local server before opening the browser
- reserve or retry callback ports cleanly
- show success/failure HTML in the browser after code capture

### Manual paste

Best fit when the provider does not redirect back into the app.

Requirements:

- show a native paste field in the auth sheet
- validate state or other anti-mixup context if the provider exposes it
- log copy/paste failures without exposing the secret

### Custom URL scheme

Best fit when the provider supports `myapp://oauth/callback`.

Requirements:

- register the scheme in `Info.plist`
- handle the callback in `onOpenURL`
- extract and validate both `code` and `state`

## Token Storage And Refresh

Required storage approach:

- Keychain only
- use `kSecAttrAccessibleWhenUnlockedThisDeviceOnly`
- delete old credentials before add/update
- clear the Keychain item completely on logout

Refresh behavior from the reference flow:

- refresh access tokens roughly five minutes before nominal expiry
- guard refresh with a lock or actor to avoid concurrent refresh races
- on refresh failure, force a clean re-login path

## Auth Resolution Order

The user reference proposes:

1. explicit user override API key
2. Keychain-backed OAuth token
3. environment variable fallback

For product UX, the app should still present subscription-backed auth as the default path even if an expert override exists.

## Debugging Checklist

- confirm PKCE verifier/challenge pair changes on every login
- confirm `state` matches on callback
- confirm the callback server is listening before browser open
- log HTTP status and sanitized error body for token exchange failures
- show token expiry and refresh status in diagnostics UI
- retry a single inference after refresh when a request races token expiry
- surface `401`, `403`, and `429` distinctly in the UI
- detect localhost callback port collisions and offer retry or manual fallback

## Local CLI Notes

The local machine already has subscription-capable or auth-capable tooling installed:

- `claude` CLI exposes `auth` and `setup-token`
- `codex` CLI exposes `login` and `logout`
- `gemini` CLI is installed locally and should be probed during implementation

That gives the app two viable auth-adapter patterns:

- embedded native OAuth flow inside the macOS app
- reuse of locally authenticated provider CLIs where the provider supports it cleanly
