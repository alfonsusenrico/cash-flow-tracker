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
          contrast: "var(--income-contrast)",
          "hover-contrast": "var(--income-hover-contrast)",
        },
        expense: {
          DEFAULT: "var(--color-expense)",
          hover: "var(--color-expense-hover)",
          soft: "var(--color-expense-soft)",
        },
        warning: {
          DEFAULT: "var(--color-warning)",
          soft: "var(--color-warning-soft)",
        },
        transfer: {
          DEFAULT: "var(--color-transfer)",
          contrast: "var(--transfer-contrast)",
        },
        investment: {
          DEFAULT: "var(--color-investment)",
          soft: "var(--color-investment-soft)",
          contrast: "var(--investment-contrast)",
        },
        canvas: "var(--canvas)",
        window: "var(--window)",
        "surface-2": "var(--surface-2)",
        "surface-3": "var(--surface-3)",
        line: "var(--line)",
        "line-strong": "var(--line-strong)",
        ink: {
          DEFAULT: "var(--ink)",
          fg: "var(--ink-fg)",
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
        emerald: {
          50: "var(--color-income-soft)",
          400: "var(--color-income)",
          500: "var(--color-income)",
          600: "var(--color-income-hover)",
          700: "var(--color-income-hover)",
        },
        rose: {
          50: "var(--color-expense-soft)",
          400: "var(--color-expense)",
          500: "var(--color-expense)",
          600: "var(--color-expense-hover)",
          700: "var(--color-expense-hover)",
        },
      },
      // Coral, emerald, amber, sky and violet (lavender) text use the darker text inks so they read at 4.5:1 on light surfaces;
      // bg-, border-, ring- and shadow- utilities keep the vivid fills from `colors`.
      textColor: {
        income: {
          DEFAULT: "var(--income-text)",
        },
        emerald: {
          400: "var(--income-text)",
          500: "var(--income-text)",
          600: "var(--income-text)",
          700: "var(--income-text)",
        },
        amber: {
          400: "var(--warning-text)",
          500: "var(--warning-text)",
          600: "var(--warning-text)",
          700: "var(--warning-text)",
        },
        sky: {
          400: "var(--transfer-text)",
          500: "var(--transfer-text)",
          600: "var(--transfer-text)",
          700: "var(--transfer-text)",
        },
        transfer: {
          DEFAULT: "var(--transfer-text)",
        },
        violet: {
          400: "var(--investment-text)",
          500: "var(--investment-text)",
          600: "var(--investment-text)",
          700: "var(--investment-text)",
        },
        investment: {
          DEFAULT: "var(--investment-text)",
        },
        expense: {
          DEFAULT: "var(--expense-text)",
        },
        primary: {
          DEFAULT: "var(--primary-text)",
        },
        rose: {
          400: "var(--expense-text)",
          500: "var(--expense-text)",
          600: "var(--expense-text)",
          700: "var(--expense-text)",
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
