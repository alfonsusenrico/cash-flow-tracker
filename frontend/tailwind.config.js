/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        canvas: {
          desktop: "var(--canvas-desktop)",
          app: "var(--canvas-app)",
          card: "var(--canvas-card)",
          subtle: "var(--canvas-subtle)",
          muted: "var(--canvas-muted)",
        },
        bg: "var(--bg)",
        surface: "var(--surface)",
        "surface-raised": "var(--surface-raised)",
        border: "var(--border)",
        "border-strong": "var(--border-strong)",
        "border-divider": "var(--border-divider)",
        text: {
          DEFAULT: "var(--text)",
          secondary: "var(--text-secondary)",
          muted: "var(--text-muted)",
          tertiary: "var(--text-tertiary)",
        },
        muted: "var(--muted)",
        accent: {
          lime: {
            DEFAULT: "var(--accent-lime)",
            hover: "var(--accent-lime-hover)",
            contrast: "var(--accent-lime-contrast)",
          },
          dark: {
            DEFAULT: "var(--accent-dark)",
            hover: "var(--accent-dark-hover)",
            contrast: "var(--accent-dark-contrast)",
          },
        },
        status: {
          success: {
            bg: "var(--status-success-bg)",
            text: "var(--status-success-text)",
          },
          warning: {
            bg: "var(--status-warning-bg)",
            text: "var(--status-warning-text)",
          },
          danger: {
            bg: "var(--status-danger-bg)",
            text: "var(--status-danger-text)",
          },
          info: {
            bg: "var(--status-info-bg)",
            text: "var(--status-info-text)",
          },
          purple: {
            bg: "var(--status-purple-bg)",
            text: "var(--status-purple-text)",
          },
        },
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
        sans: ["var(--font-jakarta)", "Plus Jakarta Sans", "Inter", "-apple-system", "BlinkMacSystemFont", "sans-serif"],
        display: ["var(--font-jakarta)", "Plus Jakarta Sans", "Inter", "-apple-system", "BlinkMacSystemFont", "sans-serif"],
        mono: ["var(--font-geist-mono)", "Geist Mono", "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      borderRadius: {
        xs: "var(--radius-xs)",
        sm: "var(--radius-sm)",
        md: "var(--radius-md)",
        lg: "var(--radius-lg)",
        btn: "var(--radius-btn)",
        card: "var(--radius-card)",
        "card-sm": "var(--radius-card-sm)",
        window: "var(--radius-window)",
        pill: "var(--radius-pill)",
      },
      boxShadow: {
        xs: "var(--shadow-xs)",
        sm: "var(--shadow-sm)",
        md: "var(--shadow-md)",
        window: "var(--shadow-window)",
      },
    },
  },
  plugins: [],
};
