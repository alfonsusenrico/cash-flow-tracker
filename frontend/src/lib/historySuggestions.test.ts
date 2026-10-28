import { describe, expect, it } from "vitest";
import { chunkSuggestionIds, defaultWindow, runNotice, windowError, type SuggestionRun } from "./historySuggestions";

const run = (error_code: string | null): SuggestionRun => ({
  id: "run", status: "completed", from_date: "2026-08-01", to_date: "2026-10-28", eligible_count: 0, selected_count: 0,
  processed_count: 0, memory_count: 0, rule_count: 0, ai_count: 0, unchanged_count: 0, rejected_before_count: 0,
  ai_calls: 0, error_code, model: null, prompt_version: null, created_at: "2026-10-28T05:00:00Z", finished_at: null,
});

describe("history suggestions helpers", () => {
  it("defaults to the last three months in local dates", () => {
    expect(defaultWindow(new Date(2026, 9, 28))).toEqual({ from: "2026-07-28", to: "2026-10-28" });
  });

  it("validates windows like the backend", () => {
    const today = new Date(2026, 9, 28);
    expect(windowError("2026-10-01", "2026-10-28", today)).toBeNull();
    expect(windowError("2026-10-10", "2026-10-01", today)).toMatch(/sebelum tanggal awal/);
    expect(windowError("2026-10-01", "2026-10-29", today)).toMatch(/melewati hari ini/);
    expect(windowError("2025-10-27", "2026-10-28", today)).toMatch(/366 hari/);
    expect(windowError("2025-10-28", "2026-10-28", today)).toBeNull();
    expect(windowError("", "2026-10-28", today)).toMatch(/Isi tanggal/);
  });

  it("explains run outcomes in Indonesian and keeps unknown codes visible", () => {
    expect(runNotice(run(null))).toBeNull();
    expect(runNotice(run("ai_disabled"))).toMatch(/AI tidak aktif/);
    expect(runNotice(run("provider_timeout"))).toContain("provider_timeout");
    expect(runNotice(run("run_interrupted"))).toMatch(/terhenti/);
    expect(runNotice(run("something_new"))).toContain("something_new");
  });

  it("splits decisions into batches of 200", () => {
    const ids = Array.from({ length: 401 }, (_, index) => `s-${index}`);
    expect(chunkSuggestionIds(ids).map((chunk) => chunk.length)).toEqual([200, 200, 1]);
  });
});
