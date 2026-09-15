## Why

Users need the ability to visually preview, toggle, and choose modern, simple design themes (Swiss Tech, Nordic Obsidian, Modern Bauhaus, Executive FinTech) directly in the live application without code changes. Furthermore, the Quick Capture transaction entry form currently displays categories as scattered, unwieldy chip clusters ("shattered tags") and needs to be replaced with an organized, ergonomic dropdown selector alongside refined button styling.

## What Changes

- **Live Theme & Preset Switcher**: Introduce a real-time interactive design system selector in the TopBar and Settings (`Swiss Tech`, `Nordic Obsidian`, `Modern Bauhaus`, `Executive FinTech`) supporting instant preview and persistent preference across light and dark modes.
- **Dynamic Modern Typography**: Dynamically load and apply modern font families tailored per preset (`Geist Sans` + `Geist Mono`, `Plus Jakarta Sans` + `JetBrains Mono`, `Outfit` + `Inter Tabular`, and `Inter Display` + `SF Mono`).
- **Unified Button Styling System**: Standardize primary, secondary, and icon buttons with modern border geometry, tactile press states, and theme-adaptive corner radii (`--radius-btn`).
- **Organized Transaction Form Inputs**: Replace the scattered category chip cluster in `QuickCaptureModal.tsx` with a clean, structured dropdown selector with icons, accompanied by a clean two-column form layout for accounts, goals, and obligations.

## Capabilities

### New Capabilities
- `theme-presets-switcher`: Interactive switcher for 4 modern minimalism design systems (Swiss Tech, Nordic Obsidian, Modern Bauhaus, Executive FinTech) with instant DOM application via `data-preset` and persistent storage.

### Modified Capabilities
- `tactile-pulse-ui`: Replace scattered category chip clusters in Quick Capture with structured dropdown selectors and streamlined form inputs.

## Impact

- **Frontend**:
  - `frontend/tokens.css`: Multi-preset CSS variable definitions (`:root[data-preset="..."]`, `.dark[data-preset="..."]`).
  - `frontend/src/app/globals.css`: Font imports for Geist, Plus Jakarta Sans, Outfit, JetBrains Mono, and Inter.
  - `frontend/src/components/layout/AppLayout.tsx`: Preset context state, `localStorage` persistence, and `data-preset` synchronization.
  - `frontend/src/components/layout/TopBar.tsx`: Interactive Style Switcher selector pill / dropdown.
  - `frontend/src/components/ui/SettingsModal.tsx`: Theme preset picker radio/segmented control.
  - `frontend/src/components/ui/QuickCaptureModal.tsx`: Organized category select dropdown replacing scattered tags, modernized buttons.
- **Backend / Database**: None (pure client-side design system and UX enhancement).
