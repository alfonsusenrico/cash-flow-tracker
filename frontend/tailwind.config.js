/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        bg: "var(--bg)",
        surface: "var(--surface)",
        "surface-raised": "var(--surface-raised)",
        border: "var(--border)",
        "border-strong": "var(--border-strong)",
        text: "var(--text)",
        "text-secondary": "var(--text-secondary)",
        muted: "var(--muted)",
        income: {
          DEFAULT: "var(--color-income)",
          hover: "var(--color-income-hover)",
          soft: "var(--color-income-soft)",
        },
        expense: {
          DEFAULT: "var(--color-expense)",
          hover: "var(--color-expense-hover)",
          soft: "var(--color-expense-soft)",
        },
        transfer: {
          DEFAULT: "var(--color-transfer)",
          hover: "var(--color-transfer-hover)",
          soft: "var(--color-transfer-soft)",
        },
        warning: {
          DEFAULT: "var(--color-warning)",
          soft: "var(--color-warning-soft)",
        },
        sidebar: "var(--sidebar)",
        "sidebar-hover": "var(--sidebar-hover)",
        "sidebar-active": "var(--sidebar-active)",
        "sidebar-text": "var(--sidebar-text)",
        "sidebar-text-active": "var(--sidebar-text-active)",
        primary: {
          DEFAULT: "var(--primary)",
          hover: "var(--primary-hover)",
          soft: "var(--primary-soft)",
          contrast: "var(--primary-contrast)",
        },
      },
      fontFamily: {
        sans: ["var(--font-jakarta)", "Plus Jakarta Sans", "-apple-system", "BlinkMacSystemFont", "sans-serif"],
        display: ["var(--font-jakarta)", "Plus Jakarta Sans", "-apple-system", "BlinkMacSystemFont", "sans-serif"],
        mono: ["var(--font-geist-mono)", "Geist Mono", "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      borderRadius: {
        xs: "var(--radius-xs)",
        sm: "var(--radius-sm)",
        md: "var(--radius-md)",
        lg: "var(--radius-lg)",
        btn: "var(--radius-btn)",
        card: "var(--radius-card)",
      },
      boxShadow: {
        sm: "var(--shadow-sm)",
        md: "var(--shadow-md)",
      },
    },
  },
  plugins: [],
};
