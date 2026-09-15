## Context

The Cash Flow Tracker frontend is built with Next.js (App Router), Tailwind CSS, and React Query. While functional and cleanly structured, the interface currently projects a cold, developer-dashboard aesthetic (dark slate backgrounds, rectangular 1px borders, rigid typography).

Users need the app to feel like modern consumer fintech apps (**Bibit**, **Cashew**, **Kazz**) where money logging is engaging, rewarding, and fun.

## Goals / Non-Goals

**Goals:**
- **Warm & Friendly Typography:** Switch application primary typeface to **Plus Jakarta Sans** with clean weights and tabular numbers for financial figures.
- **Vibrant Pop Color Palette:** Modernize semantic colors with high-energy pop tones (Electric Mint `#00D09C`, Strawberry Rose `#FF3B69`, Candy Violet `#6366F1`, Marigold `#FFB020`) and translucent candy washes.
- **Squircle & Pill Geometry:** Standardize on super-rounded card radii (`rounded-3xl` / 24px) and organic pill chips (`rounded-full`).
- **Tactile Spring Physics:** Add instant spring compression feedback (`active:scale-[0.96]`) on interactive surfaces.
- **Smooth Number Rolling Counter:** Provide a zero-dependency `useAnimatedCounter` hook for key dashboard figures (daily allowance, liquid net worth, cycle projections).
- **Smooth Progress Bar & Ring Fills:** Animate budget bars and goal rings with smooth ease-out transitions.
- **Success Celebration on Quick Capture:** Introduce a brief bouncy checkmark transition upon transaction save before modal dismissal.

**Non-Goals:**
- Heavy, sluggish 1-second page transition delays or layout-blocking loaders.
- Adding bloated animation dependencies (e.g., full Framer Motion) when pure CSS GPU transitions and lightweight RAF hooks achieve 60-120 FPS natively and securely.
- Altering any backend endpoints, database schemas, or calculation formulas.

## Decisions

### Decision 1: Plus Jakarta Sans via Next.js Google Fonts
- **Chosen:** Load `Plus_Jakarta_Sans` in `frontend/src/app/layout.tsx` using `next/font/google` and assign `--font-jakarta`.
- **Rationale:** Plus Jakarta Sans is the de facto benchmark for modern Indonesian fintech (Bibit, Stockbit, GoTo). It provides curved friendly terminals and high personality while remaining razor sharp on retina screens and supporting `font-variant-numeric: tabular-nums`.

### Decision 2: Pure CSS Hardware-Accelerated Micro-Presses
- **Chosen:** Utility `.pressable` and Tailwind `active:scale-[0.96]` with cubic-bezier timing (`cubic-bezier(0.16, 1, 0.3, 1)`).
- **Rationale:** GPU-composited transforms run smoothly without repainting or layout thrashing. Instant 150ms feedback simulates an iPhone haptic tap.

### Decision 3: Zero-Dependency `useAnimatedCounter` Hook
- **Chosen:** A custom React hook using `requestAnimationFrame` and an exponential ease-out curve over 400ms.
- **Rationale:** Completely CSP-compliant, zero bundle overhead, smoothly counts from previous value to new value when data loads or cycle changes.

### Decision 4: Tactile "Liquid Pill" & Translucent Glow Tints
- **Chosen:** Soft candy washes in dark mode (e.g., `rgba(0, 208, 156, 0.12)` for income / need, `rgba(255, 59, 105, 0.12)` for expense / want) with subtle 1px matching borders.
- **Rationale:** Eliminates flat gray boxes and replaces them with glowing, vibrant surfaces that make data pop off the dark canvas.

## Risks / Trade-offs

- **[Risk] Animation Distraction or Sluggishness:** Users logging multiple transactions quickly might get frustrated if animations take too long.
  → **Mitigation:** Keep all durations between 150ms and 400ms. Keep the transaction success burst to 400ms max, with immediate optimistic closing if preferred.
- **[Risk] Layout Shift During Number Counter:** Rolling numbers with variable character widths can cause horizontal jitter.
  → **Mitigation:** Apply `font-mono` or `font-variant-numeric: tabular-nums` to ensure equal digit glyph widths during counter ticks.
