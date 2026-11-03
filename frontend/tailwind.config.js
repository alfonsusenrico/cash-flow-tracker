/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      // Workspace tokens only (redesign-money-and-settings D11). The semantic solids take an opacity modifier
      // through color-mix, so `bg-out-solid/10` tints with the themed token; the other keys are plain variables.
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
          "fg": "var(--in-fg)",
          "bg": "var(--in-bg)",
          "solid": "color-mix(in srgb, var(--in-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-hover": "color-mix(in srgb, var(--in-solid-hover) calc(<alpha-value> * 100%), transparent)",
          "solid-fg": "var(--in-solid-fg)",
          "solid-hover-fg": "var(--in-solid-hover-fg)",
        },
        "out": {
          "fg": "var(--out-fg)",
          "bg": "var(--out-bg)",
          "solid": "color-mix(in srgb, var(--out-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-hover": "color-mix(in srgb, var(--out-solid-hover) calc(<alpha-value> * 100%), transparent)",
        },
        "tr": {
          "fg": "var(--tr-fg)",
          "bg": "var(--tr-bg)",
          "solid": "color-mix(in srgb, var(--tr-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-fg": "var(--tr-solid-fg)",
        },
        "inv": {
          "fg": "var(--inv-fg)",
          "bg": "var(--inv-bg)",
          "solid": "color-mix(in srgb, var(--inv-solid) calc(<alpha-value> * 100%), transparent)",
          "solid-fg": "var(--inv-solid-fg)",
        },
        "warn": {
          "fg": "var(--warn-fg)",
          "bg": "var(--warn-bg)",
          "solid": "color-mix(in srgb, var(--warn-solid) calc(<alpha-value> * 100%), transparent)",
        },
        "ok": {
          "fg": "var(--ok-fg)",
          "bg": "var(--ok-bg)",
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
