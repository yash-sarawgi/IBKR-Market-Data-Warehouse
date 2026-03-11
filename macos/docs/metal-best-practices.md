# Metal Best Practices

This document is the concise Apple-source companion to `metal-replatform.md`.

## Core Rule

For this product, the correct architecture is hybrid:

- SwiftUI and AppKit for window management, forms, menus, accessibility, text, and standard desktop controls
- MetalKit for dense, animated, or frequently invalidated visual surfaces

## Rendering Guidance

- Prefer `MTKView` for drawable lifecycle and frame pacing.
- Redraw on demand when the surface is idle.
- Unpause only while execution is active or the visual state is genuinely changing.
- Keep per-frame payloads small and move deterministic visualization math into testable pure Swift helpers.
- Precompile `.metallib` assets for the runnable app bundle.
- Keep a local-development fallback path only for cases where the bundle library is unavailable.

## Capability Guidance

- Gate optional effects against Apple Metal feature-family support.
- Use the Metal feature tables before assuming one GPU profile across Macs.
- Do not expand the renderer into ordinary forms, settings, or text-entry surfaces.

## Packaging Guidance

- Keep canonical shader source checked into the repo.
- Compile the shipping `.metallib` during app-bundle assembly.
- Copy the resulting library into the app bundle resources.
- Prefer loading the bundled library in production.

## Validation Guidance

- Unit test the pure Swift visualization state and transform layers.
- Run the local macOS smoke harness after wiring or changing a Metal surface.
- Use GPU frame capture and tooling when render pacing or draw correctness regresses.

## Sources

- https://developer.apple.com/metal/
- https://developer.apple.com/documentation/metal
- https://developer.apple.com/documentation/metalkit/mtkview
- https://developer.apple.com/metal/Metal-Feature-Set-Tables.pdf
- https://developer.apple.com/documentation/xcode/installing-additional-xcode-components
