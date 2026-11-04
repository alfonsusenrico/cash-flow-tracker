import { existsSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import nextConfig from "../../next.config.mjs";

describe("retired routes", () => {
  it("sends /tanya to Beranda with a permanent (308) redirect", async () => {
    const redirects = await nextConfig.redirects!();
    expect(redirects).toContainEqual({ source: "/tanya", destination: "/", permanent: true });
  });

  it("ships no Tanya page or intent code", () => {
    for (const dir of ["app/tanya", "components/tanya", "lib/tanya"]) {
      expect(existsSync(path.resolve(__dirname, "..", dir)), dir).toBe(false);
    }
  });
});
