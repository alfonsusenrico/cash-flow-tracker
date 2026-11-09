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
      // Workspace tokens only (redesign-money-and-settings D11), and only the keys a class uses (apply-markas-cleanup
      // D5): the error and warning inks. Everything else paints with `var(--…)` from the stylesheet.
      colors: {
        out: { fg: "var(--out-fg)" },
        warn: { fg: "var(--warn-fg)" },
      },
    },
  },
  plugins: [],
};
