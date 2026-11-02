import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  resolve: {
    alias: { "@": path.resolve(__dirname, "src") },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    css: true,
    // Reports and labels follow the application timezone, so tests must not depend on the runner's.
    env: { TZ: "Asia/Jakarta" },
  },
});
