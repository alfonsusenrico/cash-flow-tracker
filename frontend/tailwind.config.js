/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        sidebar: "#1a2332",
        "sidebar-hover": "#243044",
        "sidebar-active": "#1e3a2f",
        primary: {
          DEFAULT: "#16a34a",
          hover: "#15803d",
          light: "#dcfce7",
        },
        surface: "#ffffff",
        "surface-dark": "#1e293b",
        border: "#e5e7eb",
        "border-dark": "#334155",
        muted: "#6b7280",
        danger: "#dc2626",
        warning: "#f59e0b",
        info: "#3b82f6",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "sans-serif"],
      },
    },
  },
  plugins: [],
};
