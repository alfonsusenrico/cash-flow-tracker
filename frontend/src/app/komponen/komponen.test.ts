import { NextRequest } from "next/server";
import { describe, expect, it } from "vitest";
import nextConfig from "../../../next.config.mjs";
import { proxy } from "../../proxy";

// The component sheet is development-only (apply-markas-foundation D12).
describe("component sheet", () => {
  it("inlines the sheet flag, empty unless the local build sets it, and keeps its source out of the standalone output", () => {
    expect(nextConfig.env?.NEXT_PUBLIC_COMPONENT_SHEET).toBe(process.env.NEXT_PUBLIC_COMPONENT_SHEET === "1" ? "1" : "");
    expect(nextConfig.outputFileTracingExcludes?.["/komponen"]).toEqual(["./src/app/komponen/**"]);
  });

  it("answers 404 without the local flag, signed in or not", () => {
    const signedIn = new NextRequest("http://127.0.0.1/komponen", { headers: { cookie: "ledger_session=contoh" } });
    expect(proxy(signedIn).status).toBe(404);
    expect(proxy(new NextRequest("http://127.0.0.1/komponen")).status).toBe(404);
  });
});
