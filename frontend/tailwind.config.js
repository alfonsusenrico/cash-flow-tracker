/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    // Markas+ scales replace Tailwind's defaults (apply-markas-foundation D5–D7): seven sizes with their role
    // line-heights, three weights, the three radii and one layer shadow. Any other value fails typeScale.test.ts
    // or geometry.test.ts instead of rendering silently.
    fontSize: {
      xs: ["var(--fs-12)", { lineHeight: "16px" }],
      sm: ["var(--fs-14)", { lineHeight: "20px" }],
      base: ["var(--fs-16)", { lineHeight: "24px" }],
      xl: ["var(--fs-20)", { lineHeight: "28px" }],
      "2xl": ["var(--fs-24)", { lineHeight: "32px" }],
      "3xl": ["var(--fs-32)", { lineHeight: "40px" }],
      "4xl": ["var(--fs-44)", { lineHeight: "52px" }],
    },
    fontWeight: {
      normal: "400",
      medium: "500",
      semibold: "600",
    },
    borderRadius: {
      none: "0",
      sm: "var(--r-control)",
      DEFAULT: "var(--r-control)",
      md: "var(--r-control)",
      lg: "var(--r-panel)",
      xl: "var(--r-panel)",
      "2xl": "var(--r-layer)",
      "3xl": "var(--r-layer)",
      full: "9999px",
    },
    boxShadow: {
      none: "none",
      float: "var(--shadow-float)",
    },
    fontFamily: {
      sans: ["var(--font-sans)"],
      display: ["var(--font-display)"],
    },
    extend: {
      // Workspace tokens only (redesign-money-and-settings D11). Tinted semantic fills are gone (D7), so the
      // keys are plain variables without an opacity channel.
      colors: {
        canvas: "var(--canvas)",
        window: "var(--window)",
        surface: "var(--surface)",
        "surface-2": "var(--surface-2)",
        "surface-3": "var(--surface-3)",
        dialog: "var(--dialog)",
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
        in: {
          fg: "var(--in-fg)",
          bg: "var(--in-bg)",
          solid: "var(--in-solid)",
        },
        out: {
          fg: "var(--out-fg)",
          bg: "var(--out-bg)",
          solid: "var(--out-solid)",
        },
        tr: {
          fg: "var(--tr-fg)",
          bg: "var(--tr-bg)",
          solid: "var(--tr-solid)",
        },
        inv: {
          fg: "var(--inv-fg)",
          bg: "var(--inv-bg)",
          solid: "var(--inv-solid)",
        },
        warn: {
          fg: "var(--warn-fg)",
          bg: "var(--warn-bg)",
          solid: "var(--warn-solid)",
        },
        ok: {
          fg: "var(--ok-fg)",
          bg: "var(--ok-bg)",
        },
      },
    },
  },
  plugins: [],
};
