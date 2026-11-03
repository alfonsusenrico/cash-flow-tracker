/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      // Workspace tokens only (redesign-money-and-settings D11). Semantic families take an opacity modifier
      // through color-mix, so `bg-out-solid/10` tints with the themed token.
      colors: {
        canvas: "var(--canvas)",
        window: "var(--window)",
        surface: "var(--surface)",
        "surface-2": "var(--surface-2)",
        "surface-3": "var(--surface-3)",
        line: "var(--line)",
        "line-strong": "var(--line-strong)",
        text: "var(--text)",
        "text-2": "var(--text-2)",
        muted: "var(--muted)",
        ink: {
          DEFAULT: "var(--ink)",
          hover: "var(--ink-hover)",
          fg: "var(--ink-fg)",
        },
        "in": {
          "fg": "color-mix(in srgb, var(--in-fg) calc(<alpha-value> * 100%), transparent)",
          "bg": "color-mix(in srgb, var(--in-bg) calc(<alpha-value> * 100%), transparent)",
          "solid": "color-mix(in srgb, var(--in-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-hover": "color-mix(in srgb, var(--in-solid-hover) calc(<alpha-value> * 100%), transparent)",
          "solid-fg": "color-mix(in srgb, var(--in-solid-fg) calc(<alpha-value> * 100%), transparent)",
          "solid-hover-fg": "color-mix(in srgb, var(--in-solid-hover-fg) calc(<alpha-value> * 100%), transparent)",
        },
        "out": {
          "fg": "color-mix(in srgb, var(--out-fg) calc(<alpha-value> * 100%), transparent)",
          "bg": "color-mix(in srgb, var(--out-bg) calc(<alpha-value> * 100%), transparent)",
          "solid": "color-mix(in srgb, var(--out-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-hover": "color-mix(in srgb, var(--out-solid-hover) calc(<alpha-value> * 100%), transparent)",
        },
        "tr": {
          "fg": "color-mix(in srgb, var(--tr-fg) calc(<alpha-value> * 100%), transparent)",
          "bg": "color-mix(in srgb, var(--tr-bg) calc(<alpha-value> * 100%), transparent)",
          "solid": "color-mix(in srgb, var(--tr-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-fg": "color-mix(in srgb, var(--tr-solid-fg) calc(<alpha-value> * 100%), transparent)",
        },
        "inv": {
          "fg": "color-mix(in srgb, var(--inv-fg) calc(<alpha-value> * 100%), transparent)",
          "bg": "color-mix(in srgb, var(--inv-bg) calc(<alpha-value> * 100%), transparent)",
          "solid": "color-mix(in srgb, var(--inv-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-fg": "color-mix(in srgb, var(--inv-solid-fg) calc(<alpha-value> * 100%), transparent)",
        },
        "warn": {
          "fg": "color-mix(in srgb, var(--warn-fg) calc(<alpha-value> * 100%), transparent)",
          "bg": "color-mix(in srgb, var(--warn-bg) calc(<alpha-value> * 100%), transparent)",
          "solid": "color-mix(in srgb, var(--warn-solid) calc(<alpha-value> * 100%), transparent)",
        },
        "ok": {
          "fg": "color-mix(in srgb, var(--ok-fg) calc(<alpha-value> * 100%), transparent)",
          "bg": "color-mix(in srgb, var(--ok-bg) calc(<alpha-value> * 100%), transparent)",
        },
      },
      fontFamily: {
        sans: ["var(--font-geist)", "Geist", "ui-sans-serif", "system-ui", "-apple-system", "sans-serif"],
        display: ["var(--font-geist)", "Geist", "ui-sans-serif", "system-ui", "-apple-system", "sans-serif"],
        mono: ["var(--font-geist-mono)", "Geist Mono", "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      borderRadius: {
        xs: "var(--radius-xs)",
        sm: "var(--radius-sm)",
        md: "var(--radius-md)",
        lg: "var(--radius-lg)",
        btn: "var(--radius-btn)",
        window: "var(--radius-window)",
        card: "var(--radius-card)",
      },
      boxShadow: {
        xs: "var(--shadow-xs)",
        sm: "var(--shadow-sm)",
        md: "var(--shadow-md)",
        card: "var(--shadow-card)",
        float: "var(--shadow-float)",
      },
    },
  },
  plugins: [],
};
